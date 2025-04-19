import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { DynamoDBClient, BatchWriteItemCommand } from '@aws-sdk/client-dynamodb';
import { ScanCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { format, sub } from 'date-fns';
import cliProgress from 'cli-progress';
import { Command } from 'commander';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// === CLI SETUP ===
const program = new Command();
program
  .requiredOption('--source-table <name>', 'Source DynamoDB table name')
  .option('--destination-table <name>', 'Destination DynamoDB table name')
  .option('--destination-dir <path>', 'Directory to save documents to')
  .parse();

const options = program.opts( );

if (!options.destinationTable && !options.destinationDir) {
  console.error('❌ You must specify at least --destination-table or --destination-dir');
  process.exit(1);
}

// === ENV CONFIG ===
const {
  SOURCE_AWS_ACCESS_KEY_ID,
  SOURCE_AWS_SECRET_ACCESS_KEY,
  SOURCE_AWS_REGION,
  DEST_AWS_ACCESS_KEY_ID,
  DEST_AWS_SECRET_ACCESS_KEY,
  DEST_AWS_REGION,
} = process.env;

if (!SOURCE_AWS_ACCESS_KEY_ID || !SOURCE_AWS_SECRET_ACCESS_KEY || !SOURCE_AWS_REGION) {
  console.error('❌ Missing source AWS credentials in .env');
  process.exit(1);
}
if (options.destinationTable && (!DEST_AWS_ACCESS_KEY_ID || !DEST_AWS_SECRET_ACCESS_KEY || !DEST_AWS_REGION)) {
  console.error('❌ Missing destination AWS credentials in .env');
  process.exit(1);
}

// === AWS CLIENTS ===
const sourceClient = new DynamoDBClient({
  region: SOURCE_AWS_REGION,
  credentials: {
    accessKeyId: SOURCE_AWS_ACCESS_KEY_ID,
    secretAccessKey: SOURCE_AWS_SECRET_ACCESS_KEY,
  },
});
const sourceDdb = DynamoDBDocumentClient.from(sourceClient);

let destClient, destinationTableName;
if (options.destinationTable) {
  destClient = new DynamoDBClient({
    region: DEST_AWS_REGION,
    credentials: {
      accessKeyId: DEST_AWS_ACCESS_KEY_ID,
      secretAccessKey: DEST_AWS_SECRET_ACCESS_KEY,
    },
  });
  destinationTableName = options.destinationTable;
}

// === OUTPUT DIR ===
let outputDir;
if (options.destinationDir) {
  const timestamp = format(new Date(), 'yyyyMMdd_HHmm');
  outputDir = path.join(__dirname, options.destinationDir, timestamp);
  fs.mkdirSync(outputDir, { recursive: true });
}

// === SCAN FILTER DATE ===
const FILTER_DATE = format(sub(new Date(), { years: 2 }), 'yyyy-MM-dd');
const LIMIT = 100;

// === PROGRESS BAR ===
const progress = new cliProgress.SingleBar({
  format: 'Progress |{bar}| {value} pages',
  barCompleteChar: '\u2588',
  barIncompleteChar: '\u2591',
  hideCursor: true,
});
progress.start(1000, 0);

// === SCAN FUNCTION ===
async function scanAndProcess() {
  let lastEvaluatedKey = undefined;
  let page = 0;

  while (true) {
    const params = {
      TableName: options.sourceTable,
      Limit: LIMIT,
      FilterExpression: 'modified_date > :start_date',
      ExpressionAttributeValues: {
        ':start_date': FILTER_DATE,
      },
      ExclusiveStartKey: lastEvaluatedKey,
    };

    const response = await sourceDdb.send(new ScanCommand(params));
    const items = response.Items || [];

    // === WRITE TO FILES ===
    if (outputDir) {
      for (const item of items) {
        const fileName = `${item.id}_${item.profile}.json`;
        const filePath = path.join(outputDir, fileName);
        fs.writeFileSync(filePath, JSON.stringify(item, null, 2));
      }
    }

    // === WRITE TO DEST TABLE ===
    if (destClient && destinationTableName) {
      const batches = chunkItems(items, 25);
      for (const batch of batches) {
        const writeRequest = {
          RequestItems: {
            [destinationTableName]: batch.map((item) => ({
              PutRequest: { Item: item },
            })),
          },
        };
        await destClient.send(new BatchWriteItemCommand(writeRequest));
      }
    }

    progress.increment();
    lastEvaluatedKey = response.LastEvaluatedKey;
    if (!lastEvaluatedKey) break;
    page++;
  }

  progress.stop();
  console.log(`✅ Done. Pages scanned: ${page + 1}`);
}

// === Helper: Chunk array into groups of 25
function chunkItems(items, size) {
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

scanAndProcess().catch((err) => {
  console.error('❌ Error:', err);
  progress.stop();
});
