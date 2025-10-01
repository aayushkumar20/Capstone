The **Silver Layer** in the Medallion Architecture serves as the **staging and conform layer**. Data in the Silver Layer should be **cleaned, transformed, and in the lowest granularity** (i.e., row-level data) from the Bronze Layer, ready for downstream consumption and analysis.

Here's a breakdown of the steps and logic that should be applied in the Silver Layer, specifically within the `PL_LOAD_SILVER_LAYER` pipeline notebook activity, based on the provided requirements and transformation logic:

## Silver Layer Transformation Steps

The primary goal in the Silver Layer is to perform **data cleaning, standardization, and initial feature engineering** to create reliable, single-source-of-truth tables.

---

### 1. Data Ingestion and Format Conversion

* **Read Data:** Read the raw data from the **Bronze Layer** (ADLS storage).
    * **Orders and Shipments:** Ingest all multiple CSV files from the "ordersandshipment" folder.
    * **Fulfillment Data:** Ingest the JSON files.
    * **Inventory Data:** Ingest the CSV files.
* **Schema Definition & Verification:** Define the expected schema for each dataset upon reading, verify the actual schema, and check initial data types.
* **Delta Format Conversion (Write):** Write the ingested and initially processed dataframes into **Delta format** in the Silver Layer to leverage ACID properties and schema enforcement.

---

### 2. Data Quality and Cleaning

This step aligns with points 7, 9, 10, and 14 of the Transformation Logic.

* **Handle Duplicates:** Identify and remove **duplicate records** across all datasets (e.g., using `Order ID` and `Order Item ID` combination for orders/shipments).
* **Handle Null/Missing Values:**
    * Identify columns with missing values.
    * For critical identifiers (like `Order ID`, `Customer ID`), rows with nulls might be dropped or flagged.
    * For less critical attributes (like `Product Name` in some instances), fill missing values using appropriate strategies (e.g., a placeholder like 'Unknown' or imputation if statistically sound).
* **Data Consistency:** Check for and ensure data type consistency and correct format, converting strings to numerics, dates, etc., where necessary (e.g., converting `Order Time` to a proper Time/Timestamp type).
* **Invalid Data:** Identify and handle invalid data (e.g., negative `Order Quantity` or `Gross Sales`, or inconsistent text casing for categorical variables like `Product Department`). Standardize text casing (e.g., to lowercase or title case) for all `Varchar` columns to ensure consistent grouping.
* **Data Accuracy (Business Rules):** Perform checks against simple business rules (e.g., `Discount %` should be between 0 and 1, or 0 and 100 depending on storage format).

---

### 3. Feature Creation and Standardization

This step includes creating the basic features that are at the transaction/row level and not pre-aggregated, aligning with the "Feature creation" section and point 1 of the requirements.

#### A. Date and Time Features (for Orders and Shipments)

1.  **Create Full Order Date/Timestamp:**
    * Combine `Order Year`, `Order Month`, `Order Day`, and `Order Time` to create a single **Order Date/Time** column (timestamp data type).
    * *Requirement 1: Create Date time feature from day, month, year feature.*
2.  **Create Full Shipment Date:**
    * Combine `Shipment Year`, `Shipment Month`, and `Shipment Day` to create a single **Shipment Date** column (date data type).

#### B. FactOrders Row-Level Features

Features that are part of the `FactOrders` table structure will be initially created here (lowest granularity).

1.  **Shipping Time:**
    * Calculate **Shipping Time (days)**: *Shipment Date - Order Date/Time* (convert result to days/duration).
    * *Feature Creation Requirement: Shipping Time - Shipment Date - Order Date.*
2.  **Delay Shipment Flag:**
    * Create a boolean/string column **Delay Shipment**:
        * 'Late' if `Shipping Time` > `Shipment Days - Scheduled`.
        * 'On Time' if `Shipping Time` $\le$ `Shipment Days - Scheduled`.
    * *Feature Creation Requirement: Delay Shipment - Late if Shipment Day - Schedule < Shipping Time and vice versa.*
3.  **Unit Price:**
    * Calculate **Unit Price**: *Gross Sales / Order Quantity*.
    * *Feature Creation Requirement: Unit Price - Gross sales / Order Quantity.*

---

### 4. Dataframe Post-Processing and Review

This step is essential to ensure the cleaned data meets expectations, as per the remaining points in the Transformation Logic.

* **Verify Schema & Datatypes:** Re-verify the schema and check datatypes after cleaning and feature creation.
* **Summary Statistics:** Calculate and review summary statistics (min, max, mean, count) for key numerical columns (`Gross Sales`, `Profit`, `Order Quantity`, etc.).
* **Final Row/Column Count:** Verify the total number of rows and columns (e.g., comparing to the Bronze layer count to confirm only duplicates/invalid rows were removed).
* **Create Views:** Create a temporary or global **view** on the final Spark Delta DataFrames to allow for immediate SQL querying and analysis within the notebook environment.

---

## Silver Layer Output

The Silver Layer will output **cleaned, refined, and standardized Delta tables** in the lowest granularity. These tables will serve as the reliable foundation for creating the Star Schema (Dimension and Fact tables) in the Gold Layer.

The main tables expected in the Silver Layer are:

1.  **`Silver_Orders_Shipments`**: The cleaned and enhanced transaction-level data.
2.  **`Silver_Fulfillment`**: The cleaned fulfillment data.
3.  **`Silver_Inventory`**: The cleaned inventory data.