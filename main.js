import "dotenv/config";
import fs from "fs";
import path from "path";
import process from "node:process";
import { fileURLToPath } from "url";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  ScanCommand,
  DynamoDBDocumentClient,
  BatchWriteCommand,
} from "@aws-sdk/lib-dynamodb";
import { format, sub } from "date-fns";
import { Command } from "commander";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const program = new Command();
program
  .requiredOption("--source-table <name>", "Source DynamoDB table name")
  .option("--destination-table <name>", "Destination DynamoDB table name")
  .option("--destination-dir <path>", "Directory to save documents to")
  .option("--date-filter-property <name>", "Table property to filter dates by")
  .option(
    "--filter-days <days>",
    "Pair the filter to fetch documents newer than this number of days",
    600,
  )
  .parse();

const options = program.opts();

if (!options.destinationTable && !options.destinationDir) {
  console.error(
    "You must specify at least --destination-table or --destination-dir",
  );
  process.exit(1);
}

// === ENV CONFIG ===
const {
  SOURCE_AWS_ACCESS_KEY_ID,
  SOURCE_AWS_SECRET_ACCESS_KEY,
  SOURCE_AWS_REGION,
  SOURCE_AWS_ENDPOINT_URL,
  DEST_AWS_ACCESS_KEY_ID,
  DEST_AWS_SECRET_ACCESS_KEY,
  DEST_AWS_REGION,
  DEST_AWS_ENDPOINT_URL,
} = process.env;

if (
  !SOURCE_AWS_ACCESS_KEY_ID ||
  !SOURCE_AWS_SECRET_ACCESS_KEY ||
  !SOURCE_AWS_REGION
) {
  console.error("Missing source AWS credentials in .env");
  process.exit(1);
}
if (
  options.destinationTable &&
  (!DEST_AWS_ACCESS_KEY_ID || !DEST_AWS_SECRET_ACCESS_KEY || !DEST_AWS_REGION)
) {
  console.error("Missing destination AWS credentials in .env");
  process.exit(1);
}

const sourceClient = new DynamoDBClient({
  region: SOURCE_AWS_REGION,
  endpoint: SOURCE_AWS_ENDPOINT_URL,
  credentials: {
    accessKeyId: SOURCE_AWS_ACCESS_KEY_ID,
    secretAccessKey: SOURCE_AWS_SECRET_ACCESS_KEY,
  },
});
const sourceDocClient = DynamoDBDocumentClient.from(sourceClient);

let destClient, destinationTableName, destDocClient;
if (options.destinationTable) {
  destClient = new DynamoDBClient({
    region: DEST_AWS_REGION,
    endpoint: DEST_AWS_ENDPOINT_URL,
    credentials: {
      accessKeyId: DEST_AWS_ACCESS_KEY_ID,
      secretAccessKey: DEST_AWS_SECRET_ACCESS_KEY,
    },
  });
  destinationTableName = options.destinationTable;
  destDocClient = DynamoDBDocumentClient.from(destClient);
}

let outputDir;
if (options.destinationDir) {
  const timestamp = format(new Date(), "yyyyMMdd_HHmm");
  outputDir = path.join(__dirname, options.destinationDir, timestamp);
  fs.mkdirSync(outputDir, { recursive: true });
}

const filterDate = sub(new Date(), { days: options.filterDays }).toISOString();
const itemsPerPage = 100;

async function scanAndProcess() {
  let lastEvaluatedKey = undefined;
  let page = 0;
  let counter = 0;

  while (true) {
    const params = {
      TableName: options.sourceTable,
      Limit: itemsPerPage,
      ...(options.dateFilterProperty && {
        FilterExpression: `${options.dateFilterProperty} > :start_date`,
        ExpressionAttributeValues: {
          ":start_date": filterDate,
        },
      }),
      ExclusiveStartKey: lastEvaluatedKey,
    };

    const response = await sourceDocClient.send(new ScanCommand(params));
    const items = response.Items || [];

    if (outputDir) {
      for (const item of items) {
        const fileName = `${String(counter++).padStart(5, "0")}.json`;
        const filePath = path.join(outputDir, fileName);
        fs.writeFileSync(filePath, JSON.stringify(item, null, 2));
      }
      console.info(`Wrote '${items.length}' files from page '${page + 1}'`);
    }

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
        await destDocClient.send(new BatchWriteCommand(writeRequest));
      }

      console.info(
        `Saved '${items.length}' to destination table from page '${page + 1}'`,
      );
    }

    lastEvaluatedKey = response.LastEvaluatedKey;
    if (!lastEvaluatedKey) break;
    page++;
  }

  console.info(`Done. Pages scanned: ${page + 1}`);
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
  console.error("Error:", err);
});
