## 1\. Azure Setup and ADF Pipeline (Conceptual Steps)

### A. Azure Services

1.  **Azure Data Lake Storage Gen2 (ADLS Gen2):** Set up a storage account to store the raw and processed data. Create containers for the three layers: `bronze`, `silver`, and `gold`.
2.  **Azure Databricks Workspace:** Create a workspace and a cluster to run the PySpark notebooks.
3.  **Azure Data Factory (ADF):** Create an instance for orchestrating the pipelines.
4.  **Azure Logic Apps:** (Optional, for email trigger) Set up a logic app to send emails, or use the native email notification features in ADF/Databricks.

### B. ADF Pipeline - `PL_COPY_RAW_FILES` (Bronze Layer Ingestion)

1.  **`pl_copy_raw_files`:**
      * **Source:** Configure a **GitHub** or **HTTP** linked service to read the raw files.
      * **Destination (Bronze Layer):** Configure an **ADLS Gen2** linked service.
      * **Copy Activities:**
          * `ca_copy_orders_shipments`: Source: GitHub/HTTP (Folder: `ordersandshipment`, File format: CSV, Delimiter: comma). Sink: ADLS GenLS Gen2 (`bronze/ordersandshipments/`).
          * `ca_copy_fulfillment`: Source: GitHub/HTTP (File: `fulfillment.json`, File format: JSON). Sink: ADLS Gen2 (`bronze/fulfillment/`).
          * `ca_copy_inventory`: Source: GitHub/HTTP (File: `inventory.csv`, File format: CSV). Sink: ADLS Gen2 (`bronze/inventory/`).

## 2\. PySpark Notebooks for Medallion Architecture

All code will be written in PySpark (Python). The notebooks will assume a Databricks environment is connected to the ADLS Gen2 storage.

-----

## Notebook 1: `nb_load_silver_layer` (Bronze to Silver)

This notebook performs ingestion from the Bronze layer, schema definition, basic cleaning, and initial data quality checks.

### Setup and Configuration

```python
# Databricks setup: Assumes the cluster is already configured to access ADLS Gen2

# Define file paths
bronze_orders_path = "abfss://bronze@<yourstorageaccount>.dfs.core.windows.net/ordersandshipments/"
bronze_fulfillment_path = "abfss://bronze@<yourstorageaccount>.dfs.core.windows.net/fulfillment/"
bronze_inventory_path = "abfss://bronze@<yourstorageaccount>.dfs.core.windows.net/inventory/"
silver_path = "abfss://silver@<yourstorageaccount>.dfs.core.windows.net/"

# Unity Catalog/Schema definition
catalog_name = "supply_chain_catalog"
schema_name = "silver"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# Function to read and write to Delta Lake with upsert logic
def upsert_to_delta(df, table_name, unique_cols):
    """Reads a DataFrame and merges it into a Delta table using unique_cols."""
    
    # Create the target table path
    target_path = f"{silver_path}/{table_name}"
    
    # Create the merge condition
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in unique_cols])
    
    # Check if the table already exists
    try:
        # Load the existing Delta table
        delta_table = DeltaTable.forPath(spark, target_path)
        
        # Perform the MERGE operation (Upsert)
        print(f"Merging data into existing table: {table_name}")
        delta_table.alias("target") \
            .merge(
                df.alias("source"),
                merge_condition
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        
    except Exception as e:
        # If the table does not exist, simply write the data for the first time
        print(f"Creating new Delta table: {table_name}")
        df.write.format("delta").mode("overwrite").save(target_path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} USING DELTA LOCATION '{target_path}'")
        
    print(f"Data ingestion and upsert complete for {table_name}.")

```

### A. Orders and Shipments Data Processing (`silver_orders_shipments`)

```python
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *

print("--- Processing Orders and Shipments Data ---")

# 1. Define Schema for Orders and Shipments (Required step for robustness)
orders_schema = StructType([
    StructField("Order ID", LongType(), True),
    StructField("Order Item ID", IntegerType(), True),
    StructField("Order YearMonth", IntegerType(), True),
    StructField("Order Year", IntegerType(), True),
    StructField("Order Month", IntegerType(), True),
    StructField("Order Day", IntegerType(), True),
    StructField("Order Time", StringType(), True),
    StructField("Order Quantity", IntegerType(), True),
    StructField("Product Department", StringType(), True),
    StructField("Product Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Customer ID", IntegerType(), True),
    StructField("Customer Market", StringType(), True),
    StructField("Customer Region", StringType(), True),
    StructField("Customer Country", StringType(), True),
    StructField("Warehouse Country", StringType(), True),
    StructField("Shipment Year", IntegerType(), True),
    StructField("Shipment Month", IntegerType(), True),
    StructField("Shipment Day", IntegerType(), True),
    StructField("Shipment Mode", StringType(), True),
    StructField("Shipment Days - Scheduled", IntegerType(), True),
    StructField("Gross Sales", DecimalType(10, 2), True),
    StructField("Discount %", DecimalType(3, 2), True),
    StructField("Profit", DecimalType(10, 2), True)
])

# 2. Extract and Read Data
df_orders = spark.read.format("csv") \
    .option("header", "true") \
    .schema(orders_schema) \
    .load(bronze_orders_path)

# 3. Data Cleaning and Transformation (Steps 7, 9, 10)
# Standardize column names (remove spaces, special chars)
df_orders_silver = df_orders.toDF(*(col.replace(' ', '_').replace('-', '_').replace('%', 'Perc') for col in df_orders.columns))

# Remove duplicates based on primary key combination (Order ID + Order Item ID)
df_orders_silver = df_orders_silver.dropDuplicates(['Order_ID', 'Order_Item_ID'])

# Handle Nulls/Data Quality (e.g., fill or remove based on business logic)
# For simplicity, dropping rows with critical nulls (IDs, Quantity, Sales)
df_orders_silver = df_orders_silver.na.drop(subset=['Order_ID', 'Order_Item_ID', 'Order_Quantity', 'Gross_Sales', 'Profit'])

# Verify Schema and First Few Records
print("Schema:")
df_orders_silver.printSchema()
print("First 5 records:")
df_orders_silver.show(5, truncate=False)

# 4. Write to Silver Layer (Delta format with Upsert)
upsert_to_delta(df_orders_silver, "silver_orders_shipments", ["Order_ID", "Order_Item_ID"])
```

### B. Fulfillment Data Processing (`silver_fulfillment`)

```python
print("\n--- Processing Fulfillment Data ---")

# 1. Define Schema
fulfillment_schema = StructType([
    StructField("Product Name", StringType(), True),
    StructField("Warehouse Order Fulfillment (days)", FloatType(), True)
])

# 2. Extract and Read Data (JSON files)
df_fulfillment = spark.read.format("json").schema(fulfillment_schema).load(bronze_fulfillment_path)

# 3. Data Cleaning and Transformation
df_fulfillment_silver = df_fulfillment.toDF(*(col.replace(' ', '_').replace('(', '').replace(')', '') for col in df_fulfillment.columns))
df_fulfillment_silver = df_fulfillment_silver.na.drop(subset=['Product_Name']) # Drop rows with null Product Name
df_fulfillment_silver = df_fulfillment_silver.dropDuplicates(['Product_Name']) # Product Name is the unique key here

# Verify Schema and First Few Records
print("Schema:")
df_fulfillment_silver.printSchema()
print("First 5 records:")
df_fulfillment_silver.show(5, truncate=False)

# 4. Write to Silver Layer (Delta format with Upsert)
upsert_to_delta(df_fulfillment_silver, "silver_fulfillment", ["Product_Name"])
```

### C. Inventory Data Processing (`silver_inventory`)

```python
print("\n--- Processing Inventory Data ---")

# 1. Define Schema
inventory_schema = StructType([
    StructField("Product Name", StringType(), True),
    StructField("Year Month", IntegerType(), True),
    StructField("Warehouse Inventory", IntegerType(), True),
    StructField("Inventory Cost Per Unit", DecimalType(10, 5), True)
])

# 2. Extract and Read Data (CSV files)
df_inventory = spark.read.format("csv") \
    .option("header", "true") \
    .schema(inventory_schema) \
    .load(bronze_inventory_path)

# 3. Data Cleaning and Transformation
df_inventory_silver = df_inventory.toDF(*(col.replace(' ', '_') for col in df_inventory.columns))
df_inventory_silver = df_inventory_silver.na.drop(subset=['Product_Name', 'Year_Month']) # Drop rows with critical nulls
# Unique key: Product Name + Year Month
df_inventory_silver = df_inventory_silver.dropDuplicates(['Product_Name', 'Year_Month'])

# Verify Schema and First Few Records
print("Schema:")
df_inventory_silver.printSchema()
print("First 5 records:")
df_inventory_silver.show(5, truncate=False)

# 4. Write to Silver Layer (Delta format with Upsert)
upsert_to_delta(df_inventory_silver, "silver_inventory", ["Product_Name", "Year_Month"])
```

-----

## Notebook 2: `nb_load_gold_dimension_tables` (Silver to Gold - Dimensions)

This notebook creates the Dimension tables for the Gold layer.

### Setup

```python
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Define paths
silver_orders_path = f"abfss://silver@<yourstorageaccount>.dfs.core.windows.net/silver_orders_shipments/"
gold_path = "abfss://gold@<yourstorageaccount>.dfs.core.windows.net/"

# Unity Catalog/Schema definition
catalog_name = "supply_chain_catalog"
schema_name = "gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# Function to write to Gold (Dimension tables are typically overwritten or type 2 SCD, here we use overwrite for simplicity)
def write_gold_table(df, table_name, partition_cols=[]):
    target_path = f"{gold_path}/{table_name}"
    print(f"Writing Gold table: {table_name}")
    df.write.format("delta").mode("overwrite").partitionBy(*partition_cols).save(target_path)
    # Register the table in Unity Catalog
    spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} USING DELTA LOCATION '{target_path}'")
    print(f"Table {table_name} registered in Unity Catalog.")

# Read the main Silver table
df_orders_silver = spark.read.format("delta").load(silver_orders_path)
```

### A. DimProductDepartment

```python
print("--- Creating DimProductDepartment ---")
df_dim_dept = df_orders_silver.select(col("Product_Department").alias("Product_Department_Name")).distinct()

# Add Surrogate Key (Product_Department_ID)
w = Window.orderBy(monotonically_increasing_id())
df_dim_dept = df_dim_dept.withColumn("Product_Department_ID", row_number().over(w)) \
                          .withColumn("Description", col("Product_Department_Name"))

# Select final attributes
df_dim_dept = df_dim_dept.select("Product_Department_ID", "Product_Department_Name", "Description")
write_gold_table(df_dim_dept, "dim_product_department")
```

### B. DimProductCategory

```python
print("--- Creating DimProductCategory ---")
df_dim_cat = df_orders_silver.select(col("Product_Category").alias("Product_Category_Name"), "Product_Department").distinct()

# Join with DimProductDepartment to get the FK
df_dim_cat = df_dim_cat.join(
    df_dim_dept.select("Product_Department_ID", col("Product_Department_Name").alias("Product_Department")),
    on="Product_Department",
    how="inner"
).drop("Product_Department")

# Add Surrogate Key (Product_Category_ID)
w = Window.orderBy(monotonically_increasing_id())
df_dim_cat = df_dim_cat.withColumn("Product_Category_ID", row_number().over(w)) \
                       .withColumn("Description", col("Product_Category_Name"))

# Select final attributes
df_dim_cat = df_dim_cat.select("Product_Category_ID", "Product_Department_ID", "Product_Category_Name", "Description")
write_gold_table(df_dim_cat, "dim_product_category")
```

### C. DimProduct

```python
print("--- Creating DimProduct ---")
df_dim_prod = df_orders_silver.select(col("Product_Name").alias("Product_Name"), "Product_Category").distinct()

# Join with DimProductCategory to get the FK
df_dim_prod = df_dim_prod.join(
    df_dim_cat.select("Product_Category_ID", col("Product_Category_Name").alias("Product_Category")),
    on="Product_Category",
    how="inner"
).drop("Product_Category")

# Add Surrogate Key (Product_ID)
w = Window.orderBy(monotonically_increasing_id())
df_dim_prod = df_dim_prod.withColumn("Product_ID", row_number().over(w))

# Select final attributes
df_dim_prod = df_dim_prod.select("Product_ID", "Product_Category_ID", "Product_Name", lit("").alias("Description"))
write_gold_table(df_dim_prod, "dim_product")
```

### D. DimGeography

```python
print("--- Creating DimGeography ---")
df_dim_geo = df_orders_silver.select(
    col("Customer_Country").alias("Country"),
    col("Customer_Region").alias("Region"),
    col("Customer_Market").alias("Market")
).distinct()

# Add Surrogate Key (Geography_ID)
w = Window.orderBy(monotonically_increasing_id())
df_dim_geo = df_dim_geo.withColumn("Geography_ID", row_number().over(w))

write_gold_table(df_dim_geo, "dim_geography")
```

### E. DimCustomer

```python
print("--- Creating DimCustomer ---")
df_dim_cust = df_orders_silver.select(
    col("Customer_ID").alias("Customer_Data_ID"),
    col("Customer_Country"),
).distinct()

# Join with DimGeography to get the FK
df_dim_cust = df_dim_cust.join(
    df_dim_geo.select(col("Geography_ID"), col("Country").alias("Customer_Country")),
    on="Customer_Country",
    how="inner"
).drop("Customer_Country")

# Add Primary Key and other attributes
df_dim_cust = df_dim_cust.withColumnRenamed("Customer_Data_ID", "Customer_ID") \
                         .withColumn("Customer_Data_ID", col("Customer_ID")) \
                         .withColumn("Customer_Name", lit(""))

# Select final attributes and enforce distinct on Customer_ID (Data_ID)
df_dim_cust = df_dim_cust.select(
    "Customer_ID", # Assuming Customer_ID is unique across the dataset and acts as the primary key
    "Customer_Data_ID",
    "Customer_Name",
    col("Geography_ID").alias("Customer_Country_ID")
).distinct() # Use distinct to ensure one row per unique Customer_ID

write_gold_table(df_dim_cust, "dim_customer")
```

### F. DimDate (Simplified)

```python
print("--- Creating DimDate ---")

# Extract all unique date components from Order and Shipment
df_order_dates = df_orders_silver.select("Order_Year", "Order_Month", "Order_Day").distinct().withColumnRenamed("Order_Year", "Year").withColumnRenamed("Order_Month", "Month").withColumnRenamed("Order_Day", "Day")
df_shipment_dates = df_orders_silver.select("Shipment_Year", "Shipment_Month", "Shipment_Day").distinct().withColumnRenamed("Shipment_Year", "Year").withColumnRenamed("Shipment_Month", "Month").withColumnRenamed("Shipment_Day", "Day")

# Combine and get unique dates
df_all_dates = df_order_dates.union(df_shipment_dates).distinct().na.drop()

# Create Date column
df_dim_date = df_all_dates.withColumn("Date", to_date(concat_ws("-", col("Year"), col("Month"), col("Day"))))

# Add DateKey
df_dim_date = df_dim_date.withColumn("DateKey", date_format(col("Date"), "yyyyMMdd").cast(IntegerType()))

# Select final attributes
df_dim_date = df_dim_date.select("DateKey", "Year", "Month", "Day").distinct()

write_gold_table(df_dim_date, "dim_date")
```

-----

## Notebook 3: `nb_load_gold_fact_tables` (Silver to Gold - Facts and Feature Creation)

This notebook creates the Fact tables and calculates the required KPI features.

### Setup

```python
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Define paths
silver_orders_path = f"abfss://silver@<yourstorageaccount>.dfs.core.windows.net/silver_orders_shipments/"
silver_fulfillment_path = f"abfss://silver@<yourstorageaccount>.dfs.core.windows.net/silver_fulfillment/"
silver_inventory_path = f"abfss://silver@<yourstorageaccount>.dfs.core.windows.net/silver_inventory/"

# Read all necessary Gold Dimension tables
df_dim_date = spark.read.format("delta").table(f"{catalog_name}.{schema_name}.dim_date")
df_dim_cust = spark.read.format("delta").table(f"{catalog_name}.{schema_name}.dim_customer")
df_dim_prod = spark.read.format("delta").table(f"{catalog_name}.{schema_name}.dim_product")

# Function to write to Gold (Fact tables should be appended or merged for incremental loads, here we use overwrite for simplicity of first load)
def write_gold_table(df, table_name, partition_cols=[]):
    target_path = f"{gold_path}/{table_name}"
    print(f"Writing Gold table: {table_name}")
    # Using 'overwrite' for simplicity, but 'append' or 'merge' would be used in a production setting
    df.write.format("delta").mode("overwrite").partitionBy(*partition_cols).save(target_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} USING DELTA LOCATION '{target_path}'")
    print(f"Table {table_name} registered in Unity Catalog.")

# Read the main Silver table
df_orders_silver = spark.read.format("delta").load(silver_orders_path)
```

### A. FactOrders and FactShipment (Combined for simplicity, then separated)

```python
print("--- Creating FactOrders and FactShipment ---")

# Step 1: Create Date Time feature
df_orders_fact = df_orders_silver.withColumn("Order_Date", to_date(concat_ws("-", col("Order_Year"), col("Order_Month"), col("Order_Day")))) \
                                 .withColumn("Shipment_Date", to_date(concat_ws("-", col("Shipment_Year"), col("Shipment_Month"), col("Shipment_Day"))))

df_orders_fact = df_orders_fact.withColumn("Order_Date_Time", to_timestamp(concat(col("Order_Date"), lit(" "), col("Order_Time")), "yyyy-MM-dd HH:mm"))

# Step 2: Feature Creation - Shipment features
df_orders_fact = df_orders_fact.withColumn("Shipping_Time", datediff(col("Shipment_Date"), col("Order_Date")))
df_orders_fact = df_orders_fact.withColumn(
    "Delay_Shipment",
    when(col("Shipping_Time") > col("Shipment_Days___Scheduled"), lit("Late"))
    .otherwise(lit("On Time"))
)

# Step 3: Feature Creation - Business performance features
df_orders_fact = df_orders_fact.withColumn("Net_Sales", col("Gross_Sales") * (lit(1) - col("Discount_Perc")))
df_orders_fact = df_orders_fact.withColumn("Unit_Price", col("Gross_Sales") / col("Order_Quantity"))

# Step 4: Join with Dimension Tables to get Surrogate Keys
df_orders_fact = df_orders_fact.withColumn("Order_DateKey", date_format(col("Order_Date"), "yyyyMMdd").cast(IntegerType()))
df_orders_fact = df_orders_fact.withColumn("Shipment_DateKey", date_format(col("Shipment_Date"), "yyyyMMdd").cast(IntegerType()))

# Join for Product_ID
df_orders_fact = df_orders_fact.join(df_dim_prod.select("Product_ID", col("Product_Name").alias("PName")), col("Product_Name") == col("PName"), "inner").drop("PName")

# Join for Customer_ID
df_orders_fact = df_orders_fact.withColumnRenamed("Customer_ID", "Customer_Data_ID")
df_orders_fact = df_orders_fact.join(df_dim_cust.select("Customer_ID", col("Customer_Data_ID").alias("CustDataID")), col("Customer_Data_ID") == col("CustDataID"), "inner").drop("CustDataID")


# --- FactOrders Table Creation ---
df_fact_orders = df_orders_fact.select(
    row_number().over(Window.orderBy(monotonically_increasing_id())).alias("Order_PK_ID"), # New Primary Key for FactOrders
    col("Order_ID").alias("Order_Number"),
    col("Order_Item_ID").alias("Order_Item_ID"),
    col("Order_PK_ID").alias("Order_ID"), # Use the row_number as the PK (as requested in the schema)
    col("Order_DateKey"),
    col("Order_Date_Time").alias("Order_Date"),
    col("Order_Quantity"),
    col("Product_ID").alias("Product_ID"),
    col("Customer_ID").alias("Customer_ID"),
    col("Gross_Sales"),
    col("Discount_Perc").alias("Discount_Percentage"),
    col("Profit"),
    col("Net_Sales"),
    col("Unit_Price")
)

write_gold_table(df_fact_orders, "fact_orders", ["Order_DateKey"])

# --- FactShipment Table Creation ---
df_fact_shipment = df_orders_fact.select(
    row_number().over(Window.orderBy(monotonically_increasing_id())).alias("Shipment_ID"), # Primary Key
    col("Order_PK_ID").alias("Order_ID"), # Foreign Key to FactOrders
    col("Shipment_DateKey"),
    col("Shipment_Mode"),
    col("Shipment_Days___Scheduled").alias("Shipment_Days_Scheduled"),
    col("Shipping_Time"),
    col("Delay_Shipment"),
    col("Warehouse_Country")
)

write_gold_table(df_fact_shipment, "fact_shipment", ["Shipment_DateKey"])
```

### B. FactInventory

```python
print("--- Creating FactInventory ---")

df_inventory_silver = spark.read.format("delta").load(silver_inventory_path)

# Feature Creation: Storage cost
df_inventory_fact = df_inventory_silver.withColumn(
    "Storage_Cost",
    col("Inventory_Cost_Per_Unit") * col("Warehouse_Inventory")
)

# Create DateKey (Year_Month)
df_inventory_fact = df_inventory_fact.withColumn(
    "Year_Month",
    col("Year_Month").cast(StringType())
)

df_inventory_fact = df_inventory_fact.withColumn(
    "Inventory_DateKey",
    concat(col("Year_Month"), lit("01")).cast(IntegerType()) # Assume day is 01
)

# Join with DimProduct to get Product_ID
df_inventory_fact = df_inventory_fact.join(
    df_dim_prod.select("Product_ID", col("Product_Name").alias("PName")),
    col("Product_Name") == col("PName"),
    "inner"
).drop("PName", "Product_Name")

# Select final attributes
df_fact_inventory = df_inventory_fact.select(
    row_number().over(Window.orderBy(monotonically_increasing_id())).alias("Inventory_ID"), # Primary Key
    col("Product_ID"),
    col("Inventory_DateKey"),
    col("Warehouse_Inventory"),
    col("Inventory_Cost_Per_Unit"),
    col("Storage_Cost")
)

write_gold_table(df_fact_inventory, "fact_inventory", ["Inventory_DateKey"])
```

### C. FactMonthlyOrders (Pre-aggregated)

```python
print("--- Creating FactMonthlyOrders ---")

# Join FactOrders with DimDate to get Year and Month
df_monthly_orders_base = df_fact_orders.join(
    df_dim_date.select(col("DateKey").alias("Order_DateKey"), "Year", "Month"),
    on="Order_DateKey",
    how="inner"
)

# Join with FactShipment to get Delay info
df_monthly_orders_base = df_monthly_orders_base.join(
    df_fact_shipment.select("Order_ID", col("Delay_Shipment")),
    on="Order_ID",
    how="inner"
)

# Aggregate to Monthly Level
df_monthly_orders = df_monthly_orders_base.groupBy("Year", "Month").agg(
    sum("Net_Sales").alias("Total_Net_Sales"),
    sum("Profit").alias("Total_Profit"),
    countDistinct(col("Order_Number")).alias("Total_Orders"), # Total number of distinct orders
    sum(when(col("Delay_Shipment") == "Late", 1).otherwise(0)).alias("Total_Late_Orders")
)

# Feature Creation: Late Shipment Rate and Profit Margin
df_monthly_orders = df_monthly_orders.withColumn(
    "Late_Shipment_Rate",
    col("Total_Late_Orders") / col("Total_Orders")
)

df_monthly_orders = df_monthly_orders.withColumn(
    "Profit_Margin",
    col("Total_Profit") / col("Total_Net_Sales")
)

# Create Year_Month_Key
df_monthly_orders = df_monthly_orders.withColumn(
    "Year_Month_Key",
    concat(col("Year"), lpad(col("Month"), 2, '0')).cast(IntegerType())
)

# Select final attributes
df_fact_monthly_orders = df_monthly_orders.select(
    col("Year_Month_Key").alias("Monthly_Key"),
    "Total_Net_Sales",
    "Total_Profit",
    "Profit_Margin",
    "Total_Orders",
    "Total_Late_Orders",
    "Late_Shipment_Rate"
)

write_gold_table(df_fact_monthly_orders, "fact_monthly_orders", ["Monthly_Key"])
```

## 3\. Azure Data Factory Master Pipeline

### `PL_LOAD_SUPPLYCHAIN_DATA_MASTER`

1.  **Trigger:** Set a **Schedule Trigger** (`trg_daily_load`) to run at `12:05 UTC` every day.
2.  **Activities:**
      * **Execute Pipeline (`pl_copy_raw_files`):** Copy all raw data to the Bronze layer.
          * *Success Path $\rightarrow$*
      * **Notebook (`nb_load_silver_layer`):** Read from Bronze, clean, transform, and write to Silver (Delta tables).
          * *Success Path $\rightarrow$*
      * **Notebook (`nb_load_gold_dimension_tables`):** Read from Silver, create, and write Gold Dimension tables.
          * *Success Path $\rightarrow$*
      * **Notebook (`nb_load_gold_fact_tables`):** Read from Silver/Gold Dimensions, create features, and write Gold Fact tables.
          * *Success Path $\rightarrow$*
      * **Web/Logic App Activity (Success):** Trigger an email with a success message.
          * *Failure Path (from any step) $\rightarrow$*
      * **Web/Logic App Activity (Failure):** Trigger an email with a failure message, detailing the error.