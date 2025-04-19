DynamoDB Downloader
===================

This tool allows you to scan a DynamoDB table and either save the data to files or copy it to another DynamoDB table. It supports filtering data based on a modification date and provides a progress bar for tracking the scan process.

## Features

- Scan a DynamoDB table with a filter for items modified within the last two years.
- Save scanned items to JSON files in a specified directory.
- Copy scanned items to another DynamoDB table.
- Progress bar to track the scanning process.

## Prerequisites

- Node.js installed on your system.
- AWS credentials for both the source and destination DynamoDB tables (if applicable).
- A `.env` file with the following environment variables:

```plaintext
SOURCE_AWS_ACCESS_KEY_ID=<your-source-access-key-id>
SOURCE_AWS_SECRET_ACCESS_KEY=<your-source-secret-access-key>
SOURCE_AWS_REGION=<your-source-region>
DEST_AWS_ACCESS_KEY_ID=<your-destination-access-key-id> (optional)
DEST_AWS_SECRET_ACCESS_KEY=<your-destination-secret-access-key> (optional)
DEST_AWS_REGION=<your-destination-region> (optional)
```

## Installation

1. Clone the repository.
2. Install dependencies:

   ```bash
   npm install
   ```

3. Create a `.env` file in the root directory and populate it with your AWS credentials.

## Usage

Run the script using the following command:

```bash
node main.js --source-table <source-table-name> [options]
```

### Required Options

- `--source-table <name>`: The name of the source DynamoDB table to scan.

### Optional Options

- `--destination-table <name>`: The name of the destination DynamoDB table to copy the scanned items to.
- `--destination-dir <path>`: The directory where scanned items will be saved as JSON files.

### Example Commands

1. **Save items to a directory:**

   ```bash
   node main.js --source-table MySourceTable --destination-dir ./output
   ```

2. **Copy items to another DynamoDB table:**

   ```bash
   node main.js --source-table MySourceTable --destination-table MyDestinationTable
   ```

3. **Save items to a directory and copy to another table:**

   ```bash
   node main.js --source-table MySourceTable --destination-dir ./output --destination-table MyDestinationTable
   ```

## How It Works

1. The script scans the source DynamoDB table in pages of 100 items, filtering for items modified within the last two years.
2. If `--destination-dir` is specified, the items are saved as JSON files in the specified directory, organized by timestamp.
3. If `--destination-table` is specified, the items are copied to the destination DynamoDB table in batches of 25.

## Progress Tracking

A progress bar is displayed during the scan, showing the number of pages processed.

## Error Handling

If any required AWS credentials or options are missing, the script will terminate with an error message.

## License

This project is licensed under the MIT License.