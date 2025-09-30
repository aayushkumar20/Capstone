
For the Silver layer pipeline (`PL_LOAD_SILVER_LAYER`), the notebook will:
- Read raw data from Bronze (multiple CSVs for Orders/Shipments, JSON for Fulfillment, CSVs for Inventory).
- Apply transformations from your "[1] Transformation Logic" (e.g., schema verification, cleaning, data quality checks).
- Create features like datetime from day/month/year, shipment delay indicators.
- Write cleaned data to Silver layer as Delta tables in ADLS.

Follow naming standards: Notebook as `nb_BronzeToSilver`.

#### Prerequisites
- Azure Databricks workspace linked to your ADLS (Azure Data Lake Storage Gen2).
- ADF linked service for Databricks (Author > Manage > Linked Services > New > Azure Databricks).
- Bronze data in ADLS (e.g., `abfss://bronze@<storage-account>.dfs.core.windows.net/ordersandshipment/*.csv`, `abfss://bronze@<storage-account>.dfs.core.windows.net/fulfillment/*.json`, etc.).
- Unity Catalog enabled for Delta tables (for lineage and metadata).
- Mount ADLS in Databricks or use ABFSS paths for access.

#### Step 1: Create the Notebook in Azure Databricks
1. **Access Databricks Workspace**:
   - In the Azure portal, go to your Databricks resource > Launch Workspace.
   - In the workspace sidebar, click **Workspace** > **Create** > **Notebook**.
   - Name: `nb_BronzeToSilver` (per naming standards: `nb_<notebookname>`).
   - Language: **Python** (for PySpark).
   - Cluster: Attach an existing cluster (create one if needed: Compute > Create Compute > e.g., Standard_DS3_v2, Spark 3.5+).

2. **Write PySpark Code in the Notebook**:
   - The code will handle all datasets: Orders/Shipments (multiple CSVs), Fulfillment (JSON), Inventory (CSVs).
   - Steps follow your transformation logic: Define schemas, read data, verify/clean/validate, create features, write to Silver as Delta.
   - Use Delta format for ACID compliance and Unity Catalog for registration.

   Here's a complete sample PySpark script. Paste it into notebook cells (one cell per major section for modularity). Adjust paths, schemas, and storage account details.

   ```python
   # Cell 1: Imports and Setup
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import col, concat, lit, to_date, to_timestamp, datediff, when, count, sum, avg, max, min, desc
   from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
   from delta.tables import DeltaTable

   spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

   # ADLS paths (replace with your storage account)
   bronze_container = "abfss://bronze@<your-storage-account>.dfs.core.windows.net/"
   silver_container = "abfss://silver@<your-storage-account>.dfs.core.windows.net/"

   # Mount if needed (optional, for easier paths)
   dbutils.fs.mount(source=bronze_container[:-1], mount_point="/mnt/bronze", extra_configs={...})  # Add configs for auth

   # Cell 2: Define Schemas (from dataset descriptions)
   # Orders and Shipments Schema (CSV)
   orders_schema = StructType([
       StructField("Order ID", IntegerType(), True),
       StructField("Order Item ID", IntegerType(), True),
       StructField("Order YearMonth", IntegerType(), True),
       StructField("Order Year", IntegerType(), True),
       StructField("Order Month", IntegerType(), True),
       StructField("Order Day", IntegerType(), True),
       StructField("Order Time", StringType(), True),  # Time as string, e.g., 'HH:MM:SS'
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

   # Fulfillment Schema (JSON)
   fulfillment_schema = StructType([
       StructField("Product Name", StringType(), True),
       StructField("Warehouse Order Fulfillment (days)", DoubleType(), True)
   ])

   # Inventory Schema (CSV)
   inventory_schema = StructType([
       StructField("Product Name", StringType(), True),
       StructField("Year Month", IntegerType(), True),  # e.g., YYYYMM
       StructField("Warehouse Inventory", IntegerType(), True),
       StructField("Inventory Cost Per Unit", DoubleType(), True)
   ])

   # Cell 3: Read from Bronze and Verify Schema
   # Orders/Shipments: Handle multiple CSVs
   df_orders_bronze = spark.read.schema(orders_schema).option("header", "true").csv(bronze_container + "ordersandshipment/*.csv")
   df_orders_bronze.printSchema()  # Verify schema
   df_orders_bronze.show(5)  # Verify first records
   print(f"Orders rows: {df_orders_bronze.count()}, columns: {len(df_orders_bronze.columns)}")

   # Fulfillment: JSON
   df_fulfillment_bronze = spark.read.schema(fulfillment_schema).json(bronze_container + "fulfillment/*.json")
   df_fulfillment_bronze.printSchema()
   df_fulfillment_bronze.show(5)

   # Inventory: CSV
   df_inventory_bronze = spark.read.schema(inventory_schema).option("header", "true").csv(bronze_container + "inventory/*.csv")
   df_inventory_bronze.printSchema()
   df_inventory_bronze.show(5)

   # Cell 4: Data Cleaning and Validation (Transformation Logic)
   # Cache DataFrames
   df_orders_bronze.cache()
   df_fulfillment_bronze.cache()
   df_inventory_bronze.cache()

   # Remove duplicates
   df_orders_clean = df_orders_bronze.dropDuplicates()
   df_fulfillment_clean = df_fulfillment_bronze.dropDuplicates()
   df_inventory_clean = df_inventory_bronze.dropDuplicates()

   # Handle nulls (e.g., fill numeric with 0, strings with 'Unknown')
   df_orders_clean = df_orders_clean.fillna({
       "Order Quantity": 0, "Gross Sales": 0.0, "Discount %": 0.0, "Profit": 0.0,
       "Product Department": "Unknown", "Product Category": "Unknown", "Product Name": "Unknown"
   })

   # Validate business rules (e.g., positive quantities, valid dates)
   df_orders_clean = df_orders_clean.filter(col("Order Quantity") > 0).filter(col("Gross Sales") >= 0)

   # Check missing values summary
   missing_counts = df_orders_clean.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_orders_clean.columns])
   missing_counts.show()

   # Summary stats, max/min
   df_orders_clean.describe().show()
   for col_name in ["Order Quantity", "Gross Sales", "Profit"]:
       print(f"{col_name} max: {df_orders_clean.agg(max(col(col_name))).collect()[0][0]}")
       print(f"{col_name} min: {df_orders_clean.agg(min(col(col_name))).collect()[0][0]}")

   # Similar cleaning for other DFs...

   # Cell 5: Feature Creation (from Requirements)
   # 1. Datetime feature
   df_orders_clean = df_orders_clean.withColumn("Order Date", to_date(concat(
       col("Order Year").cast("string"), lit("-"), col("Order Month").cast("string").rlike("^[1-9]$").when(True, concat(lit("0"), col("Order Month").cast("string"))).otherwise(col("Order Month").cast("string")),
       lit("-"), col("Order Day").cast("string").rlike("^[1-9]$").when(True, concat(lit("0"), col("Order Day").cast("string"))).otherwise(col("Order Day").cast("string"))
   ), "yyyy-MM-dd"))
   df_orders_clean = df_orders_clean.withColumn("Order Datetime", to_timestamp(concat(col("Order Date").cast("string"), lit(" "), col("Order Time")), "yyyy-MM-dd HH:mm:ss"))

   df_orders_clean = df_orders_clean.withColumn("Shipment Date", to_date(concat(
       col("Shipment Year").cast("string"), lit("-"), col("Shipment Month").cast("string").rlike("^[1-9]$").when(True, concat(lit("0"), col("Shipment Month").cast("string"))).otherwise(col("Shipment Month").cast("string")),
       lit("-"), col("Shipment Day").cast("string").rlike("^[1-9]$").when(True, concat(lit("0"), col("Shipment Day").cast("string"))).otherwise(col("Shipment Day").cast("string"))
   ), "yyyy-MM-dd"))

   # 2. Shipment features
   df_orders_clean = df_orders_clean.withColumn("Shipping Time", datediff(col("Shipment Date"), col("Order Date")))
   df_orders_clean = df_orders_clean.withColumn("Delay Shipment", when(col("Shipping Time") > col("Shipment Days - Scheduled"), "Late").otherwise("On Time"))

   # Inventory: Add storage cost
   df_inventory_clean = df_inventory_clean.withColumn("Storage Cost", col("Inventory Cost Per Unit") * col("Warehouse Inventory"))

   # Other features as needed (e.g., Unit Price for Gold prep)

   # Cell 6: Write to Silver as Delta Tables
   # Orders/Shipments to Silver
   df_orders_clean.write.format("delta").mode("overwrite").save(silver_container + "orders_silver")
   spark.sql("CREATE TABLE IF NOT EXISTS silver.orders USING DELTA LOCATION '{}'".format(silver_container + "orders_silver"))  # Register in Unity Catalog

   # Fulfillment to Silver
   df_fulfillment_clean.write.format("delta").mode("overwrite").save(silver_container + "fulfillment_silver")
   spark.sql("CREATE TABLE IF NOT EXISTS silver.fulfillment USING DELTA LOCATION '{}'".format(silver_container + "fulfillment_silver"))

   # Inventory to Silver
   df_inventory_clean.write.format("delta").mode("overwrite").save(silver_container + "inventory_silver")
   spark.sql("CREATE TABLE IF NOT EXISTS silver.inventory USING DELTA LOCATION '{}'".format(silver_container + "inventory_silver"))

   # Uncache
   df_orders_bronze.unpersist()
   # ...

   # Cell 7: Create Temp View for SQL Queries and Insights
   df_orders_clean.createOrReplaceTempView("orders_view")
   spark.sql("SELECT Product Department, AVG(Order Quantity) AS AvgQty FROM orders_view GROUP BY Product Department ORDER BY AvgQty DESC").show()  # Example insight: Avg order qty by dept

   print("Silver layer processing complete. Insights: [Add your EDA findings here, e.g., top products, null rates]")
   ```

   - **Explanation**:
     - **Schemas**: Enforce data types to prevent issues.
     - **Cleaning**: Handles duplicates, nulls, validation (e.g., positive values).
     - **Features**: Creates datetime, shipment delay, storage cost (prep for KPIs like Late Shipment Rate in Gold).
     - **Delta/Unity Catalog**: Ensures lineage (e.g., track transformations via Delta history).
     - **Insights**: End with EDA (e.g., summary stats, patterns) as per transformation logic #16.

3. **Test the Notebook**:
   - Run all cells in Databricks.
   - Check Delta tables in Unity Catalog (Catalog > silver schema).
   - Verify lineage: Use `DESCRIBE HISTORY silver.orders` in a SQL cell.

#### Step 2: Integrate the Notebook into ADF Pipeline
1. **Create Pipeline in ADF**:
   - Author > Pipelines > + New pipeline.
   - Name: `pl_LoadSilverLayer` (per standards: `pl_<pipelinename>`).

2. **Add Notebook Activity**:
   - Drag **Databricks Notebook** activity to the canvas.
   - Name: `RunSilverNotebook`.
   - Settings:
     - Linked service: Your Databricks linked service.
     - Notebook path: Browse to `/Workspace/nb_BronzeToSilver`.
     - Base parameters (optional): Pass dynamic values, e.g., `bronze_path` as `@pipeline().parameters.bronze_path`.
   - Add dependencies if chaining from Bronze pipeline.

3. **Add Web Activity for Email Notification**:
   - Drag **Web** activity after the Notebook.
   - URL: Your Logic App URL (create in Azure Logic Apps: Trigger on HTTP request, send email via Outlook/Office 365 connector).
   - Method: POST.
   - Body: `{"message": "Silver layer load successful"}`.
   - For failure: Use **On failure** path with another Web activity for error email.

4. **Validate, Publish, and Schedule**:
   - Validate the pipeline.
   - Publish all.
   - Add trigger: `trg_DailySilverLoad` (Schedule > 12:05 UTC daily).
   - In master pipeline `pl_LoadSupplyChainDataMaster`, add an **Execute Pipeline** activity to call this after Bronze.