Key concepts:
- **Binary format**: Ensures files (CSV/JSON) are copied as-is without parsing.
- **Wildcard filtering**: Selects all CSV and JSON files from the source container root.
- **Dynamic folder path**: Uses ADF expressions to append the UTC date (e.g., `20250930` for September 30, 2025).
- **PreserveHierarchy**: Maintains original filenames in the sink.

This setup copies all matching files in one activity run. No ForEach loop is needed unless you have complex logic (e.g., per-file transformations).

### Prerequisites
- An ADF instance with access to your Azure Storage Account.
- A linked service for the storage account (Author > Linked services > New > Azure Blob Storage > Test connection).

### Step-by-Step Process in Azure Portal GUI

1. **Create Source Dataset**:
   - Go to **Author** tab > **Datasets** > **+ New dataset**.
   - Search for and select **Azure Blob Storage**.
   - Name it `SourceBlobDataset`.
   - Select your storage **Linked service**.
   - Set **File format** to **Binary** (this copies files without schema parsing).
   - Under **Connection** tab:
     - **File path**: 
       - Container: Select your source container (e.g., `source-container`).
       - Directory: `/` (root of the container).
       - File: Leave blank.
   - Publish the dataset.

2. **Create Sink Dataset**:
   - Repeat Step 1, but name it `SinkBlobDataset`.
   - Set **File format** to **Binary**.
   - Under **Connection** tab:
     - Container: `RAW`.
     - Directory: Click **Add dynamic content** (fx icon) and enter the expression:
       ```
       INVENTORY/@{utcnow('yyyyMMdd')}
       ```
       - This dynamically creates a folder like `INVENTORY/20250930/` based on pipeline run time (UTC).
     - File: Leave blank (filename will be preserved from source).
   - Publish the dataset.

3. **Create the Pipeline**:
   - Go to **Author** tab > **Pipelines** > **+ New pipeline**.
   - Name it `CopyFilesToRawPipeline`.
   - In the canvas, search for and drag **Copy data** activity to the canvas.
   - Name the activity `CopyToRawActivity`.

4. **Configure Copy Activity - Source Tab**:
   - **Source dataset**: Select `SourceBlobDataset`.
   - **Source options**:
     - Check **Wildcard file path**.
     - **Wildcard folder path**: Leave blank (assumes files are in root; adjust if in a subfolder, e.g., `input/`).
     - **Wildcard file name**: `*.csv,*.json` (this matches all CSV and JSON files; ADF supports comma-separated patterns for multiple extensions).
   - Leave other settings default (e.g., Recursive: true if source has subfolders).

5. **Configure Copy Activity - Sink Tab**:
   - **Sink dataset**: Select `SinkBlobDataset` (this injects the dynamic date folder).
   - **Sink settings**:
     - **Copy behavior**: Select **PreserveHierarchy** (this copies files with original names into the dynamic folder without flattening or renaming).
   - Leave other settings default.

6. **Validate and Publish**:
   - Click **Validate** (top toolbar) to check for errors.
   - Click **Publish all** to save.

7. **Test the Pipeline**:
   - Click **Debug** (top toolbar) to run a test execution.
   - Monitor the output in the **Output** tab: Look for `sourceFileNames` and `sinkFileNames` arrays to confirm files were copied with preserved names.
   - After success, go to your storage account > Containers > `RAW` > Browse to verify structure (e.g., `INVENTORY/20250930/yourfile.csv`).

8. **Schedule/Trigger (Optional)**:
   - In the pipeline canvas, click **Add trigger** > **New/Edit** > Create a **Tumbling window** or **Schedule** trigger (e.g., daily at midnight UTC) to run automatically.
   - Set start date/time as needed.

### Relevant ADF Expressions ("Code")
These are used in dynamic content fields (fx icon). ADF expressions are like lightweight code snippets evaluated at runtime.

| Field | Expression | Description |
|-------|------------|-------------|
| Sink Directory Path | `INVENTORY/@{utcnow('yyyyMMdd')}` | Appends current UTC date as folder (e.g., `INVENTORY/20250930`). Use `utcnow()` for UTC; adjust to `addhours(utcnow(), 8, 'yyyyMMdd')` if needing a timezone offset. |
| Source Wildcard File Name | `*.csv,*.json` | Filters only CSV/JSON; for all files, use `*`. |
| (Optional) Add Source Filename as Metadata Column | In Source > Additional columns: Key=`SourceFileName`, Value=`@{$$FILENAME}` | Attaches original filename as a column if you later parse files (not needed for binary copy). |

### Troubleshooting Tips
- **No files copied**: Verify wildcard matches (e.g., test with `*.csv` alone). Check activity output for `effectivePath` logs.
- **Random filenames in sink**: Ensure Binary format and PreserveHierarchy are set; avoid Data Flow if not transforming data.
- **Date folder not created**: Confirm expression syntax (no quotes around `yyyyMMdd`). Test in a variable: Pipeline > Variables > New > Value: `@{utcnow('yyyyMMdd')}`.
- **Multiple extensions issue**: If comma wildcard fails, create two Copy activities (one for CSV, one for JSON) in parallel.
- **Permissions**: Ensure ADF managed identity or service principal has Storage Blob Data Contributor role on the account.
