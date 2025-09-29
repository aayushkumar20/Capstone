## ADF Pipeline Implementation

### 1\. Setup (Linked Services and Datasets)

Before creating the pipeline, ensure you have the necessary linked services and datasets.

  * **Linked Service:** Create an **Azure Blob Storage** or **Azure Data Lake Storage Gen2** Linked Service pointing to your storage account (where both the source and RAW containers are).
  * **Source Dataset (Generic):** Create a generic **Binary** dataset named `Source_Binary_DS`.
      * Point it to your **Source container**.
      * **Do not** specify a file path or file name. We will pass this dynamically.
      * Add **two parameters** to this dataset: `SourceFolder` (string) and `SourceFile` (string).
      * In the **Connection** tab of the dataset, use these parameters for the file path:
          * **Container:** `Source` (hardcoded or parameterized)
          * **Directory:** `@dataset().SourceFolder` (Leave blank for the initial setup, we will use it in the pipeline)
          * **File:** `@dataset().SourceFile`
  * **Sink Dataset (Generic):** Create a generic **Binary** dataset named `Sink_Binary_DS`.
      * Point it to your **RAW container**.
      * Add **three parameters** to this dataset: `SinkFolder` (string), `SinkSubFolder` (string), and `SinkFile` (string).
      * In the **Connection** tab of the dataset, use these parameters for the file path:
          * **Container:** `RAW` (hardcoded or parameterized)
          * **Directory:** `@dataset().SinkFolder`
          * **File:** `@dataset().SinkFile`
          * *(Note: The `SinkSubFolder` parameter will be used in the pipeline activity's directory structure).*

-----

### 2\. Create the Pipeline

Create a new pipeline in Azure Data Factory.

#### A. Get Metadata Activity

1.  Drag and drop a **Get Metadata** activity onto the canvas. Name it something like `GetFileList`.
2.  Go to the **Dataset** tab and select your `Source_Binary_DS`.
3.  For the dataset parameters:
      * **SourceFolder:** Leave this blank if your files are in the root of the Source container, or enter the subdirectory path if they are in one (e.g., `files/in/here`).
      * **SourceFile:** Leave this blank.
4.  Go to the **Field list** tab, click **New**, and select **Child Items**. This will return a list of all files and folders in the source path.

#### B. ForEach Activity

1.  Drag and drop a **ForEach** activity onto the canvas. Connect the success output (green line) of the `GetFileList` activity to the `ForEach` activity. Name it something like `IterateFiles`.

2.  Go to the **Settings** tab.

3.  Set **Items** to the dynamic content expression that retrieves the list of files from the `GetFileList` output:

    | Setting | Dynamic Content Expression |
    | :--- | :--- |
    | **Items** | `@activity('GetFileList').output.childItems` |

4.  *(Optional but Recommended: Set **Sequential** to **True** for better control over small batches, or leave as **False** for parallel processing if performance is critical and your file-to-file logic is simple).*

#### C. Copy Data Activity (Inside ForEach)

1.  Click the **Edit** pencil icon on the `IterateFiles` activity to enter its internal activities view.
2.  Drag and drop a **Copy Data** activity inside the ForEach loop. Name it something like `CopyFileToRAW`.

##### Source Configuration

1.  Go to the **Source** tab.

2.  Select the **Source Dataset**: `Source_Binary_DS`.

3.  Set the parameters for the source file dynamically:

    | Parameter | Dynamic Content Expression | Purpose |
    | :--- | :--- | :--- |
    | **SourceFolder** | Leave blank (or use the original source folder path if applicable) | The folder containing the files. |
    | **SourceFile** | `@item().name` | The current file name from the ForEach loop. |

4.  For **File Path Type**, select **Wildcard file path**.

      * **Wildcard file name:** `@item().name`

##### Sink Configuration

1.  Go to the **Sink** tab.

2.  Select the **Sink Dataset**: `Sink_Binary_DS`.

3.  Set the parameters for the destination file path. This is where you construct the dynamic path: `RAW/INVENTORY/(UTC Date YYYYMMDD)/File name + Extension`.

    | Parameter | Dynamic Content Expression | Purpose |
    | :--- | :--- | :--- |
    | **SinkFolder** | `@concat('INVENTORY/', formatDateTime(utcnow(), 'yyyyMMdd'))` | Creates the `INVENTORY/YYYYMMDD` folder path. |
    | **SinkSubFolder** | Leave blank | Not needed for this specific path structure. |
    | **SinkFile** | `@item().name` | Retains the original file name and extension. |

4.  For **Copy behavior**, you can typically use **None** (default). Since the date folder changes daily, you won't overwrite existing files unless a file with the same name runs multiple times in the same UTC day.

5.  **Preserve metadata:** In the **Settings** tab of the Copy Data activity, you may want to check the **Preserve** box and select **Last modified date** if you need to keep that information.

-----

## Dynamic Content Code Breakdown

The core of your solution lies in the dynamic expressions.

### A. ForEach Items Expression (Activity: `IterateFiles`)

This expression tells the ForEach loop which items to iterate over, taken from the `GetFileList` output.

```json
@activity('GetFileList').output.childItems
```

### B. Sink Folder Path Expression (Activity: `CopyFileToRAW` \> Sink Dataset Parameter `SinkFolder`)

This expression concatenates the static folder name (`INVENTORY/`) with the current UTC date formatted as `YYYYMMDD`.

```json
@concat('INVENTORY/', formatDateTime(utcnow(), 'yyyyMMdd'))
```

  * `utcnow()`: Gets the current Coordinated Universal Time (UTC) date and time. It's best practice to use UTC for consistent logging and partitioning.
  * `formatDateTime(..., 'yyyyMMdd')`: Formats the date part into your desired `YYYYMMDD` string.
  * `concat(...)`: Joins the strings together.

### C. Sink File Name Expression (Activity: `CopyFileToRAW` \> Sink Dataset Parameter `SinkFile`)

This expression ensures the original file name is used in the destination.

```json
@item().name
```

  * `@item()`: Refers to the current item (file) being processed in the `ForEach` loop.
  * `.name`: Accesses the name property of that item, which contains the full file name and extension (e.g., `data1.csv`).

-----

## Summary of Pipeline Flow

1.  **GetFileList**: Reads the **Source** container and gets a list of all files/folders (`childItems`).
2.  **IterateFiles**: Loops through each item returned by `GetFileList`.
3.  **CopyFileToRAW**:
      * **Source** is set to the specific file using `@item().name`.
      * **Sink** constructs the destination path dynamically: `RAW/INVENTORY/YYYYMMDD/filename.ext`.
      * This process repeats for every file, effectively copying each file to its uniquely dated folder in the **RAW** container.