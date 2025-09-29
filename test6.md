Assumptions:
- You have an Azure subscription with:
  - Azure Data Lake Storage Gen2 (ADLS Gen2) account (e.g., named `supplychainstorage`).
  - Azure Databricks workspace (e.g., named `supplychaindb`).
  - Azure Data Factory (ADF) instance (e.g., named `supplychainadf`).
  - Unity Catalog enabled in Databricks (go to Databricks UI > Catalog > Enable Unity Catalog if not).
- Datasets are uploaded to ADLS Gen2 in containers: `source` (for raw files like orders_and_shipments.csv, fulfillment.json—assuming it's JSON as "ISON" is likely a typo, inventory.csv).
- Folder structure in ADLS: `/source/ordersandshipment/` for multiple CSVs, `/source/fulfillment.json`, `/source/inventory.csv`.
- We'll create containers: `bronze`, `silver`, `gold` in ADLS.
- Use PySpark for all processing (imports like `from pyspark.sql import SparkSession`, `from pyspark.sql.functions import *`).
- For unique identifiers/surrogate keys, we'll use `monotonically_increasing_id()` or hash-based keys, and handle updates with Delta Lake's merge (upsert) for future data.
- Email notifications: Use ADF Web Activity with Azure Logic Apps (set up a simple Logic App for email).
- Scheduling: In ADF UI.
- Visualizations: At the end, I'll guide on Databricks Dashboard (since PowerBI integration requires license; use Databricks SQL for queries).

### Step 1: Set Up Azure Resources via UI
1. **ADLS Gen2 Setup** (Azure Portal > Storage Accounts > Your ADLS > Containers):
   - Create containers: `bronze`, `silver`, `gold`.
   - Upload datasets to `source` container:
     - `/source/ordersandshipment/orders_and_shipments.csv` (and any other CSVs).
     - `/source/fulfillment.json` (assuming JSON format with fields like {"Product Name": "...", "Warehouse Order Fulfillment (days)": 8.3}).
     - `/source/inventory.csv`.

2. **Databricks Setup** (Azure Portal > Databricks > Your Workspace > Launch Workspace):
   - Create a cluster (UI > Compute > Create Cluster): Single node, Runtime 13.3 LTS (Scala 2.12, Spark 3.4.1), enable Unity Catalog.
   - In Databricks UI > Catalog > Create Catalog: Name it `supplychain_catalog`.
   - Inside catalog, create Schema: `supplychain_schema`.
   - Mount ADLS to Databricks (in a notebook):
     ```python
     dbutils.fs.mount(
       source = "wasbs://source@supplychainstorage.blob.core.windows.net/",
       mount_point = "/mnt/source",
       extra_configs = {"fs.azure.account.key.supplychainstorage.blob.core.windows.net": "your-storage-key"}
     )
     # Repeat for bronze, silver, gold containers, e.g., /mnt/bronze, etc.
     ```
     - Get storage key from Azure Portal > Storage > Access Keys.

3. **ADF Setup** (Azure Portal > Data Factories > Your ADF > Launch Studio):
   - In ADF Studio UI > Manage > Linked Services: Create linked service for ADLS (e.g., `LS_ADLS`), Databricks (e.g., `LS_Databricks`—link to your workspace, use access token from Databricks User Settings).
   - For email: Create Azure Logic App (Azure Portal > Logic Apps > Create). In Logic App Designer: Trigger "When a HTTP request is received", Action "Send an email (V2)" (connect to Outlook/Office365). Save and copy the HTTP POST URL for success/failure.

4. **Unity Catalog Registration**: We'll register Delta tables in notebooks.

### Step 2: Create Databricks Notebooks
Create 3 notebooks in Databricks UI (Workflows > Notebooks > Create Notebook):
- `nb_load_bronze` (for Bronze loading—actually done in ADF copy, but notebook for validation).
- `nb_load_silver` (transformations and cleaning).
- `nb_load_gold` (dimension/fact creation, aggregations, features).

All notebooks start with imports:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
spark = SparkSession.builder.appName("SupplyChain").getOrCreate()
```

#### Notebook 1: nb_load_silver (Read from Bronze, Clean, Transform to Silver)
This reads raw from Bronze (Delta format), applies transformations from [1] Transformation Logic, cleans data, handles multiples CSVs/JSON, writes to Silver as Delta.

Code:
```python
# Define schemas based on dataset description
orders_schema = StructType([
    StructField("Order ID", IntegerType(), True),
    StructField("Order Item ID", IntegerType(), True),
    StructField("Order YearMonth", IntegerType(), True),
    StructField("Order Year", IntegerType(), True),
    StructField("Order Month", IntegerType(), True),
    StructField("Order Day", IntegerType(), True),
    StructField("Order Time", StringType(), True),  # Time as string
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
    StructField("Gross Sales", DoubleType(), True),
    StructField("Discount %", DoubleType(), True),
    StructField("Profit", DoubleType(), True)
])

fulfillment_schema = StructType([
    StructField("Product Name", StringType(), True),
    StructField("Warehouse Order Fulfillment (days)", DoubleType(), True)
])

inventory_schema = StructType([
    StructField("Product Name", StringType(), True),
    StructField("Year Month", IntegerType(), True),
    StructField("Warehouse Inventory", IntegerType(), True),
    StructField("Inventory Cost Per Unit", DoubleType(), True)
])

# Step 1-2: Read data from Bronze (assuming ADF copied to /mnt/bronze/ordersandshipment/*.csv, etc.)
# For multiple CSVs in ordersandshipment
orders_df = spark.read.schema(orders_schema).csv("/mnt/bronze/ordersandshipment/*.csv", header=True)

fulfillment_df = spark.read.schema(fulfillment_schema).json("/mnt/bronze/fulfillment.json")

inventory_df = spark.read.schema(inventory_schema).csv("/mnt/bronze/inventory.csv", header=True)

# Step 3: Verify schema
orders_df.printSchema()
fulfillment_df.printSchema()
inventory_df.printSchema()

# Step 4: Check datatypes (already enforced by schema)

# Step 5: Cache
orders_df.cache()
fulfillment_df.cache()
inventory_df.cache()

# Step 6: Verify first few records
orders_df.show(5)
fulfillment_df.show(5)
inventory_df.show(5)

# Step 7: Clean - Remove duplicates, nulls, invalid
orders_df = orders_df.dropDuplicates().na.drop(how="any")  # Drop rows with any null
# Invalid: e.g., ensure quantities > 0
orders_df = orders_df.filter(col("Order Quantity") > 0)
# Similar for others
fulfillment_df = fulfillment_df.dropDuplicates().na.drop(how="any").filter(col("Warehouse Order Fulfillment (days)") > 0)
inventory_df = inventory_df.dropDuplicates().na.drop(how="any").filter(col("Warehouse Inventory") >= 0)

# Step 8: Validate against business rules (e.g., shipment year >= order year)
orders_df = orders_df.filter(col("Shipment Year") >= col("Order Year"))

# Step 9: Fill missing (if any, but we dropped; example imputation)
orders_df = orders_df.na.fill({"Discount %": 0.0})

# Step 10: Consistency - Cast if needed (schema already handles)
# Format dates (create datetime feature as per requirements)
orders_df = orders_df.withColumn("Order Date", concat_ws("-", col("Order Year"), col("Order Month"), col("Order Day")))
orders_df = orders_df.withColumn("Order Datetime", to_timestamp(concat(col("Order Date"), lit(" "), col("Order Time")), "yyyy-MM-dd HH:mm"))
orders_df = orders_df.withColumn("Shipment Date", to_date(concat_ws("-", col("Shipment Year"), col("Shipment Month"), col("Shipment Day"))))

inventory_df = inventory_df.withColumn("Year Month Date", to_date(concat(lit(substring(col("Year Month"), 1, 4)), lit("-"), substring(col("Year Month"), 5, 2), lit("-01"))))  # Assume month start

# Step 11: Verify rows/cols
print("Orders rows:", orders_df.count(), "cols:", len(orders_df.columns))
# Similar for others

# Step 12: Summary stats
orders_df.describe().show()

# Step 13: Max/min
orders_df.agg(max("Gross Sales"), min("Gross Sales")).show()

# Step 14: Duplicates in key cols
orders_df.groupBy("Order ID").count().filter("count > 1").show()

# Step 15: Create temp view for SQL
orders_df.createOrReplaceTempView("orders_view")
spark.sql("SELECT * FROM orders_view LIMIT 5").show()

# Step 16: Preprocessing insights (EDA)
# E.g., average order quantity
spark.sql("SELECT AVG(`Order Quantity`) FROM orders_view").show()
# Inventory segmentation (e.g., high/low inventory products)
inventory_df.groupBy("Product Name").agg(avg("Warehouse Inventory").alias("Avg Inventory")).orderBy(desc("Avg Inventory")).show()

# Write to Silver as Delta (lowest granularity, cleaned)
orders_df.write.format("delta").mode("overwrite").save("/mnt/silver/orders")
fulfillment_df.write.format("delta").mode("overwrite").save("/mnt/silver/fulfillment")
inventory_df.write.format("delta").mode("overwrite").save("/mnt/silver/inventory")

# For updates in future: Use merge (upsert) on unique keys
# Example for orders: deltaTable = DeltaTable.forPath(spark, "/mnt/silver/orders")
# deltaTable.alias("target").merge(new_df.alias("source"), "target.`Order ID` = source.`Order ID`").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

#### Notebook 2: nb_load_gold (From Silver to Gold: Dimensions, Facts, Features)
Read from Silver, normalize to star schema, create features, aggregate for FactMonthlyOrders, write to Gold as Delta, register in Unity Catalog.

Code:
```python
# Read from Silver
orders_df = spark.read.format("delta").load("/mnt/silver/orders")
fulfillment_df = spark.read.format("delta").load("/mnt/silver/fulfillment")
inventory_df = spark.read.format("delta").load("/mnt/silver/inventory")

# A. Normalize to Dimensions/Facts
# DimProductDepartment
dim_dept = orders_df.select("Product Department").distinct() \
    .withColumn("Product Department ID", monotonically_increasing_id()) \
    .withColumnRenamed("Product Department", "Product Department Name") \
    .withColumn("Description", col("Product Department Name"))  # Copy if no desc

# DimProductCategory
dim_cat = orders_df.select("Product Category", "Product Department").distinct() \
    .join(dim_dept, col("Product Category.Product Department") == col("dim_dept.Product Department Name"), "inner") \
    .select("Product Category", "Product Department ID") \
    .withColumn("Product Category ID", monotonically_increasing_id()) \
    .withColumnRenamed("Product Category", "Product Category Name") \
    .withColumn("Description", col("Product Category Name"))

# DimProduct
dim_prod = orders_df.select("Product Name", "Product Category").distinct() \
    .join(dim_cat, col("Product Name.Product Category") == col("dim_cat.Product Category Name"), "inner") \
    .select("Product Name", "Product Category ID") \
    .withColumn("Product ID", monotonically_increasing_id()) \
    .withColumn("Description", col("Product Name"))

# DimCustomer
dim_cust = orders_df.select("Customer ID", "Customer Country", "Customer Region", "Customer Market").distinct() \
    .withColumn("Customer ID Surrogate", monotonically_increasing_id()) \
    .withColumnRenamed("Customer ID", "Customer Data ID") \
    .withColumn("Customer Name", lit(""))  # Blank

# DimGeography
dim_geo = dim_cust.select("Customer Country", "Customer Region", "Customer Market").distinct() \
    .withColumn("Geography ID", monotonically_increasing_id()) \
    .withColumnRenamed("Customer Country", "Country") \
    .withColumnRenamed("Customer Region", "Region") \
    .withColumnRenamed("Customer Market", "Market")

# Update DimCustomer with Geography ID
dim_cust = dim_cust.join(dim_geo, ["Customer Country", "Customer Region", "Customer Market"], "inner") \
    .drop("Customer Country", "Customer Region", "Customer Market")

# DimDate (from orders and shipments)
dates = orders_df.select("Order Date").union(orders_df.select("Shipment Date")).distinct() \
    .withColumn("Year", year(col("Order Date"))) \
    .withColumn("Month", month(col("Order Date"))) \
    .withColumn("Day", dayofmonth(col("Order Date"))) \
    .withColumnRenamed("Order Date", "Full Date") \
    .withColumn("Date Key", date_format(col("Full Date"), "yyyyMMdd").cast(IntegerType()))

# FactOrders (with features)
fact_orders = orders_df.withColumn("Order ID Surrogate", monotonically_increasing_id()) \
    .withColumnRenamed("Order ID", "Order Number") \
    .withColumn("Order Date Key", date_format(col("Order Datetime"), "yyyyMMdd").cast(IntegerType())) \
    .join(dim_prod, "Product Name", "left") \
    .join(dim_cust, col("orders_df.Customer ID") == col("dim_cust.Customer Data ID"), "left") \
    .select("Order ID Surrogate", "Order Number", "Order Item ID", "Order Date Key", "Order Datetime", 
            "Order Quantity", "Product ID", "Customer ID Surrogate as Customer ID", 
            "Gross Sales", "Discount %", "Profit")

# Feature: Shipping Time = Shipment Date - Order Date
fact_orders = fact_orders.withColumn("Shipping Time", datediff(col("Shipment Date"), col("Order Date")))

# Feature: Delay Shipment (Late if Shipping Time > Shipment Days - Scheduled)
fact_orders = fact_orders.withColumn("Delay Shipment", when(col("Shipping Time") > col("Shipment Days - Scheduled"), "Late").otherwise("On Time"))

# Feature: Net Sales = Gross Sales - (Discount % * Gross Sales)
fact_orders = fact_orders.withColumn("Net Sales", col("Gross Sales") - (col("Discount %") * col("Gross Sales")))

# Feature: Unit Price = Gross Sales / Order Quantity
fact_orders = fact_orders.withColumn("Unit Price", col("Gross Sales") / col("Order Quantity"))

# FactShipment
fact_shipment = orders_df.withColumn("Shipment ID", monotonically_increasing_id()) \
    .join(fact_orders, col("orders_df.Order ID") == col("fact_orders.Order Number"), "inner") \
    .select("Shipment ID", "Order ID Surrogate as Order ID", 
            date_format(col("Shipment Date"), "yyyyMMdd").cast(IntegerType()).alias("Shipment Date Key"), 
            "Shipment Mode", "Shipment Days - Scheduled")

# FactInventory (join with fulfillment)
fact_inventory = inventory_df.join(fulfillment_df, "Product Name", "left") \
    .join(dim_prod, "Product Name", "left") \
    .select("Product ID", "Year Month Date as Date", date_format(col("Year Month Date"), "yyyyMMdd").cast(IntegerType()).alias("Date Key"),
            "Warehouse Inventory", "Inventory Cost Per Unit", "Warehouse Order Fulfillment (days)")

# Feature: Storage Cost = Inventory Cost Per Unit * Warehouse Inventory
fact_inventory = fact_inventory.withColumn("Storage Cost", col("Inventory Cost Per Unit") * col("Warehouse Inventory"))

# FactMonthlyOrders (aggregated)
fact_monthly = fact_orders.groupBy(year("Order Datetime").alias("Year"), month("Order Datetime").alias("Month")) \
    .agg(sum("Net Sales").alias("Total Net Sales"),
         sum("Profit").alias("Total Profit"),
         (sum("Profit") / sum("Net Sales")).alias("Profit Margin"),
         count("Order Number").alias("Total Orders"),
         sum(when(col("Delay Shipment") == "Late", 1).otherwise(0)).alias("Late Orders"))

# Feature: Late Shipment Rate = Late Orders / Total Orders
fact_monthly = fact_monthly.withColumn("Late Shipment Rate", col("Late Orders") / col("Total Orders"))

# Write to Gold as Delta
dim_dept.write.format("delta").mode("overwrite").save("/mnt/gold/dim_product_department")
# Repeat for all dims and facts: dim_cat, dim_prod, dim_cust, dim_geo, dates (as dim_date), fact_orders, fact_shipment, fact_inventory, fact_monthly

# Register in Unity Catalog (for lineage)
spark.sql("CREATE SCHEMA IF NOT EXISTS supplychain_catalog.supplychain_schema")
spark.sql("CREATE TABLE supplychain_catalog.supplychain_schema.dim_product_department USING DELTA LOCATION '/mnt/gold/dim_product_department'")
# Repeat for all tables. Lineage is auto-tracked in Unity Catalog.

# For updates: Use merge as in Silver example, on surrogate keys.
```

### Step 3: Set Up ADF Pipelines (UI in ADF Studio)
1. **pl_copy_raw_files**:
   - Activities > Copy Data: Source = GitHub? Wait, no GitHub—use ADLS source container.
   - Source: LS_ADLS, Dataset ds_raw_orders (folder /source/ordersandshipment/*.csv), etc.
   - Sink: LS_ADLS, Dataset ds_bronze_orders (/bronze/ordersandshipment), format CSV to Delta? No, copy raw, then in notebook convert.
   - Copy for fulfillment, inventory.

2. **pl_load_silver_layer**:
   - Activities > Notebook: Link to LS_Databricks, run nb_load_silver.

3. **pl_load_gold_layer**:
   - Activities > Notebook: Run nb_load_gold.
   - Add Web Activity: URL = Logic App HTTP URL, body = {"message": "Gold Load Success"} for success.
   - On failure: Another Web for failure email.

4. **pl_load_supplychain_data_master**:
   - Execute Pipeline activities: Sequentially run pl_copy_raw_files > pl_load_silver_layer > pl_load_gold_layer.

5. **Trigger**: Triggers > New: trg_daily_load, recurrence every day at 12:05 UTC.

6. **Notifications**: In each pipeline, use If Condition or Web on success/failure.

### Step 4: Visualizations in Databricks Dashboard
- Databricks UI > SQL > Create Query: Use SQL on Gold tables, e.g., for Sales Manager: SELECT Year, Month, Total Net Sales FROM fact_monthly.
- Create Dashboard: Add visualizations (line for sales over time, bar for top products).
- Answer questions: E.g., Business Performance Q1: SELECT SUM(`Total Net Sales`), SUM(`Total Profit`), AVG(`Profit Margin`) FROM fact_monthly.
- For at least 2 per section: Implement queries in dashboard (e.g., pie for customer distribution, line for inventory over time).