// scan-dynamodb.js
import fs from 'fs';
import readline from 'readline';
import path from 'path';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { ScanCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { format, formatISO, sub } from 'date-fns';
import { fileURLToPath } from 'url';

const client = new DynamoDBClient({});
const ddb = DynamoDBDocumentClient.from(client);

const TABLE_NAME = 'profiles';
const LIMIT = 200;
const FILTER_DATE = formatISO(sub(new Date(), { years: 2 }));
// const FILTER_DATE = '2023-11-30T09:43:24.495Z';

// const rl = readline.createInterface({
//   input: process.stdin,
//   output: process.stdout,
// });

// function ask(question) {
//   return new Promise((resolve) => rl.question(question, resolve));
// }

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const folderName = format(new Date(), 'yyyyMMdd_HHmm');
const outputDir = path.join(__dirname, 'out', folderName);
// const now = new Date();
// const folderName = now.toISOString().replace(/[-:]/g, '').slice(0, 13).replace('T', '_'); // e.g., 20250417_1832
// const outputDir = path.join(__dirname, 'out', folderName);
fs.mkdirSync(outputDir, { recursive: true });

async function scanTable() {
  let lastEvaluatedKey = undefined;
  let counter = 0;

  while (true) {
    const params = {
      TableName: TABLE_NAME,
      Limit: LIMIT,
      FilterExpression: 'lastModifiedDate > :start_date',
      ExpressionAttributeValues: {
        ':start_date': FILTER_DATE,
      },
      ExclusiveStartKey: lastEvaluatedKey,
    };
    // console.debug(params);

    const response = await ddb.send(new ScanCommand(params));
    const items = response.Items || [];

    const fileName = `${outputDir}/page_${counter}.json`;
    fs.writeFileSync(fileName, JSON.stringify(items, null, 2));
    console.log(`Saved page ${counter} with ${items.length} items to ${fileName}.`);

    lastEvaluatedKey = response.LastEvaluatedKey;

    if (!lastEvaluatedKey) {
      console.log('No more data. Done.');
      break;
    }

    // const cont = await ask('Pull next page? (y/n): ');
    // if (cont.toLowerCase() !== 'y') {
    //   console.log('Stopped by user.');
    //   break;
    // }

    counter++;
  }

//   rl.close();
}

scanTable().catch((err) => {
  console.error('Error scanning table:', err);
//   rl.close();
});
