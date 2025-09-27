## **Section 1: Set Up GitHub Repository and Upload Datasets**
This is the source for raw data. Use GitHub web GUI—no code needed.

1. **Create the Repo**:
   - Go to github.com and sign in (or create account).
   - Click "+" > New repository.
   - Repository name: `supply-chain-capstone`.
   - Description: "Capstone project for e-commerce supply chain".
   - Visibility: Private (or public).
   - Add README: Yes.
   - Click "Create repository".

2. **Upload Datasets Using GitHub GUI**:
   - In the repo, click "Add file" > "Upload files".
   - Create folder structure:
     - Drag/create folder `data/ordersandshipment/` and upload multiple CSVs (e.g., orders_2015.csv, orders_2016.csv, orders_2017.csv). Each CSV should have headers matching the fields (e.g., Order ID, Order Item ID, etc.).
     - Folder `data/fulfillment/` and upload JSON files (e.g., fulfillment.json with array of { "Product Name": "...", "Warehouse Order Fulfillment (days)": 5.0 }).
     - Folder `data/inventory/` and upload CSVs (e.g., inventory.csv with Product Name, Year Month, etc.).
   - Commit changes: Message "Upload raw datasets".

3. **Add Other Documents**:
   - Create folder `docs/`.
   - Upload a blank PPT (use Fractal template if provided; otherwise, download a standard one and name it `capstone_presentation.pptx`).
   - Later, add screenshots here.
   - Commit: "Add docs folder".

4. **For ZIP Submission Later**: At the end, go to repo > Code > Download ZIP.

Your repo URL: `https://github.com/yourusername/supply-chain-capstone`.

---

## **Section 2: Set Up Azure Resources via Portal GUI**
Use portal.azure.com.

1. **Create Resource Group**:
   - Search "Resource groups" > Create.
   - Subscription: Your sub.
   - Name: `supply-chain-rg`.
   - Region: East US (or closest).
   - Click Review + create > Create.

2. **Create ADLS Gen2 Storage Account**:
   - Search "Storage accounts" > Create.
   - Basics: Resource group `supply-chain-rg`, Name `supplychainadls` (unique), Region same as RG, Performance Standard, Redundancy LRS.
   - Advanced: Enable hierarchical namespace (for ADLS Gen2).
   - Click Review + create > Create.
   - After creation, go to the storage > Containers > New container: `bronze`, `silver`, `gold` (private access).

3. **Get ADLS Access Key for Later**:
   - In storage account > Access keys > Show keys > Copy key1 (save securely for Databricks secrets).

4. **Create Azure Databricks Workspace**:
   - Search "Azure Databricks" > Create.
   - Basics: Resource group `supply-chain-rg`, Workspace name `supply-chain-databricks`, Region same, Pricing tier Premium (for Unity Catalog).
   - Click Review + create > Create.
   - After, click "Launch Workspace" to open Databricks UI.

5. **Set Up Databricks Cluster**:
   - In Databricks Workspace > Compute > Create compute.
   - Name: `supply-chain-cluster`.
   - Cluster mode: Single node (for cost savings).
   - Databricks runtime: 13.3 LTS (Scala 2.12, Spark 3.5.0).
   - Enable autoscaling: No.
   - Click Create compute.

6. **Set Up Unity Catalog in Databricks**:
   - In Databricks > Catalog (left menu) > Create catalog.
   - Name: `supply_chain_catalog`.
   - Storage location: Browse > Select your ADLS account > `gold` container > Create.
   - Inside catalog > Create schema: Name `supply_chain_schema`.

7. **Create Secrets in Databricks for ADLS**:
   - In Databricks > New > Notebook > Python.
   - Paste and run:
     ```
     dbutils.secrets.createScope("supply_chain_secrets")
     dbutils.secrets.put("supply_chain_secrets", "adls_key", "paste-your-adls-key1-here")
     ```
   - Note: If error, ensure Premium tier.

8. **Create Azure Data Factory (ADF)**:
   - Search "Data factories" > Create.
   - Basics: Resource group `supply-chain-rg`, Name `supply-chain-adf`, Region same.
   - Git: Enable, GitHub, Authorize your account, Repo `yourusername/supply-chain-capstone`, Branch main.
   - Click Review + create > Create.
   - After, click "Launch Studio" to open ADF Studio.

9. **Create Logic App for Email Notifications**:
   - Search "Logic Apps" > Create.
   - Basics: Resource group `supply-chain-rg`, Name `supply-chain-notifications`, Region same, Plan Consumption.
   - Click Review + create > Create.
   - After creation, go to Logic App > Designer > Blank Logic App.
   - Trigger: When a HTTP request is received.
   - Add parameter: JSON schema `{ "properties": { "message": { "type": "string" }, "email": { "type": "string" } } }`.
   - Action: Send an email (v2) > Connect Office 365 or Gmail > To: `@triggerBody()?['email']`, Subject: "Pipeline Status", Body: `@triggerBody()?['message']`.
   - Save > Copy the HTTP POST URL (for ADF Web Activity later).

---

## **Section 3: ADF Pipeline for Bronze Layer - PL_COPY_RAW_FILES**
In ADF Studio (adf.azure.com).

1. **Create Linked Services**:
   - Linked services > New > HTTP.
     - Name: `ls_github_http`.
     - Base URL: `https://raw.githubusercontent.com/yourusername/supply-chain-capstone/main/`.
     - Authentication: Anonymous.
   - New > Azure Data Lake Storage Gen2.
     - Name: `ls_adls_gen2`.
     - Authentication: Account key > Paste ADLS key.
     - Account name: `supplychainadls`.
     - Test connection.

2. **Create Datasets**:
   - Datasets > New > HTTP > DelimitedText (for CSV).
     - Name: `ds_github_orders`.
     - Linked service: `ls_github_http`.
     - File path: `data/ordersandshipment` (folder), File: `*.csv` (wildcard).
     - First row as header: Yes.
     - Import schema: From connection/store (or manual if needed).
   - Similar: `ds_github_fulfillment` > HTTP > JSON, Path `data/fulfillment/*.json`.
   - `ds_github_inventory` > HTTP > DelimitedText, Path `data/inventory/*.csv`.
   - Sink Datasets: New > Azure Data Lake Storage Gen2 > DelimitedText.
     - `ds_bronze_orders`: Linked `ls_adls_gen2`, Folder `bronze/ordersandshipment`, File `*.csv` (dynamic or static).
     - Similar for `ds_bronze_fulfillment` (format JSON or Parquet if converting), `ds_bronze_inventory`.

3. **Create Pipeline PL_COPY_RAW_FILES**:
   - Pipelines > New pipeline > Name `PL_COPY_RAW_FILES`.
   - Activities > Move and transform > Copy data.
     - Name: `copy_orders`.
     - Source: `ds_github_orders` > Wildcard path `data/ordersandshipment/*.csv`.
     - Sink: `ds_bronze_orders` > Folder `bronze/ordersandshipment/`, File name option: From pattern (e.g., @{item().name}).
   - Add Copy for fulfillment and inventory similarly.
   - Validate and Debug (run with sample data).

4. **Test**: Publish all > Trigger now. Check ADLS > bronze container for files.

Take screenshot of pipeline run success.

---

## **Section 4: Databricks Notebooks for Silver Layer - nb_load_silver_layer**
In Databricks Workspace.

1. **Create Notebook nb_load_silver_layer**:
   - New > Notebook > Name `nb_load_silver_layer`, Language Python, Attach to `supply-chain-cluster`.

2. **Paste and Run This Code** (handles reading, schema, cleaning, features for Silver):
   ```
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import col, to_date, concat, lit, when, datediff, regexp_replace
   from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

   spark = SparkSession.builder.appName("LoadSilverLayer").getOrCreate()

   # Get secret and configure ADLS
   adls_key = dbutils.secrets.get(scope="supply_chain_secrets", key="adls_key")
   spark.conf.set("fs.azure.account.key.supplychainadls.dfs.core.windows.net", adls_key)

   bronze_path = "abfss://bronze@supplychainadls.dfs.core.windows.net/"
   silver_path = "abfss://silver@supplychainadls.dfs.core.windows.net/"

   # Orders Schema
   orders_schema = StructType([
       StructField("Order ID", IntegerType(), True), StructField("Order Item ID", IntegerType(), True),
       StructField("Order YearMonth", IntegerType(), True), StructField("Order Year", IntegerType(), True),
       StructField("Order Month", IntegerType(), True), StructField("Order Day", IntegerType(), True),
       StructField("Order Time", TimestampType(), True), StructField("Order Quantity", IntegerType(), True),
       StructField("Product Department", StringType(), True), StructField("Product Category", StringType(), True),
       StructField("Product Name", StringType(), True), StructField("Customer ID", IntegerType(), True),
       StructField("Customer Market", StringType(), True), StructField("Customer Region", StringType(), True),
       StructField("Customer Country", StringType(), True), StructField("Warehouse Country", StringType(), True),
       StructField("Shipment Year", IntegerType(), True), StructField("Shipment Month", IntegerType(), True),
       StructField("Shipment Day", IntegerType(), True), StructField("Shipment Mode", StringType(), True),
       StructField("Shipment Days - Scheduled", IntegerType(), True), StructField("Gross Sales", FloatType(), True),
       StructField("Discount %", FloatType(), True), StructField("Profit", FloatType(), True)
   ])

   # Read and Process Orders
   df_orders = spark.read.schema(orders_schema).option("header", "true").csv(bronze_path + "ordersandshipment/*.csv")
   df_orders.cache()
   df_orders.printSchema()
   df_orders.show(5)
   print(f"Total rows: {df_orders.count()}")

   # Cleaning
   df_orders = df_orders.dropDuplicates()
   df_orders = df_orders.fillna(0, subset=["Order Quantity", "Gross Sales", "Discount %", "Profit"])
   df_orders = df_orders.fillna("Unknown", subset=["Product Department", "Product Category", "Product Name", "Customer Market", "Customer Region", "Customer Country"])
   df_orders = df_orders.filter((col("Order Quantity") > 0) & (col("Gross Sales") >= 0))
   df_orders.describe().show()
   df_orders.agg({"Gross Sales": "max", "Gross Sales": "min"}).show()
   duplicates = df_orders.groupBy("Order ID").count().filter("count > 1")
   duplicates.show()

   # Special Cleaning
   df_orders = df_orders.withColumn("Customer Country", regexp_replace(col("Customer Country"), "[^a-zA-Z0-9 ]", ""))
   df_orders = df_orders.withColumn("Order Date", to_date(concat(col("Order Year"), lit("-"), col("Order Month"), lit("-"), col("Order Day"))))
   df_orders = df_orders.withColumn("Shipment Date", to_date(concat(col("Shipment Year"), lit("-"), col("Shipment Month"), lit("-"), col("Shipment Day"))))
   df_orders = df_orders.withColumn("Shipping Time", datediff(col("Shipment Date"), col("Order Date")))
   df_orders = df_orders.filter((col("Shipping Time") >= 0) & (col("Shipping Time") <= 28))

   # Features
   df_orders = df_orders.withColumn("Order DateTime", concat(col("Order Date"), lit(" "), col("Order Time")))
   df_orders = df_orders.withColumn("Delay Shipment", when(col("Shipping Time") > col("Shipment Days - Scheduled"), "Late").otherwise("On Time"))
   df_orders = df_orders.withColumn("Net Sales", col("Gross Sales") - (col("Discount %") * col("Gross Sales")))
   df_orders = df_orders.withColumn("Unit Price", col("Gross Sales") / col("Order Quantity"))

   # Fulfillment Schema and Process
   fulfillment_schema = StructType([StructField("Product Name", StringType(), True), StructField("Warehouse Order Fulfillment (days)", FloatType(), True)])
   df_fulfillment = spark.read.schema(fulfillment_schema).json(bronze_path + "fulfillment/*.json")
   df_fulfillment = df_fulfillment.dropDuplicates().fillna({"Warehouse Order Fulfillment (days)": 0.0})

   # Inventory
   inventory_schema = StructType([StructField("Product Name", StringType(), True), StructField("Year Month", IntegerType(), True), StructField("Warehouse Inventory", IntegerType(), True), StructField("Inventory Cost Per Unit", FloatType(), True)])
   df_inventory = spark.read.schema(inventory_schema).option("header", "true").csv(bronze_path + "inventory/*.csv")
   df_inventory = df_inventory.dropDuplicates().fillna(0)
   # Handle inconsistencies: Keep only matching products
   matching_products = [row['Product Name'] for row in df_orders.select("Product Name").distinct().collect()]
   df_inventory = df_inventory.filter(col("Product Name").isin(matching_products))

   # Write to Silver Delta
   df_orders.write.format("delta").mode("overwrite").save(silver_path + "orders")
   df_fulfillment.write.format("delta").mode("overwrite").save(silver_path + "fulfillment")
   df_inventory.write.format("delta").mode("overwrite").save(silver_path + "inventory")

   # SQL View and EDA
   df_orders.createOrReplaceTempView("silver_orders")
   spark.sql("SELECT * FROM silver_orders LIMIT 5").show()
   spark.sql("SELECT AVG(Net Sales) AS avg_sales, AVG(Profit) AS avg_profit FROM silver_orders").show()
   ```

3. **Test**: Run all cells. Check ADLS silver container for Delta files (use Storage Explorer in Portal).

Take screenshot.

---

## **Section 5: ADF Pipeline for Silver Layer - PL_LOAD_SILVER_LAYER**
In ADF Studio.

1. **Create Linked Service for Databricks**:
   - Linked services > New > Azure Databricks.
     - Name: `ls_databricks`.
     - Workspace URL: From Databricks (e.g., https://adb-xxx.azuredatabricks.net).
     - Authentication: Access token (generate in Databricks > User settings > Generate new token).
     - Cluster ID: From Databricks compute > Copy cluster ID.

2. **Create Pipeline PL_LOAD_SILVER_LAYER**:
   - Pipelines > New > Name `PL_LOAD_SILVER_LAYER`.
   - Activities > Azure Databricks > Notebook.
     - Name: `run_silver_notebook`.
     - Linked service: `ls_databricks`.
     - Notebook path: Browse > Select `nb_load_silver_layer`.

3. **Test**: Publish > Debug.

---

## **Section 6: Databricks Notebooks for Gold Layer - nb_load_gold_layer**
Create one notebook for all Gold transformations.

1. **Create nb_load_gold_layer** (Python, attach cluster).

2. **Paste and Run This Code** (creates dims/facts, KPIs):
   ```
   from pyspark.sql.functions import monotonically_increasing_id, year, month, dayofmonth, date_format, sum, count, when
   from pyspark.sql.types import DateType

   gold_path = "abfss://gold@supplychainadls.dfs.core.windows.net/"

   # Read Silver
   df_orders = spark.read.format("delta").load(silver_path + "orders")
   df_fulfillment = spark.read.format("delta").load(silver_path + "fulfillment")
   df_inventory = spark.read.format("delta").load(silver_path + "inventory")

   # DimProductDepartment
   dim_product_dept = df_orders.select(col("Product Department").alias("ProductDepartmentName")).distinct() \
       .withColumn("ProductDepartmentID", monotonically_increasing_id()) \
       .withColumn("Description", col("ProductDepartmentName")) \
       .select("ProductDepartmentID", "ProductDepartmentName", "Description")

   # DimProductCategory
   dim_product_category = df_orders.select("Product Category", "Product Department").distinct() \
       .join(dim_product_dept, col("Product Department") == col("ProductDepartmentName"), "inner") \
       .withColumn("ProductCategoryID", monotonically_increasing_id()) \
       .withColumn("Description", col("Product Category")) \
       .select("ProductCategoryID", "ProductDepartmentID", col("Product Category").alias("ProductCategoryName"), "Description")

   # DimProduct
   dim_product = df_orders.select("Product Name", "Product Category").distinct() \
       .join(dim_product_category, col("Product Category") == col("ProductCategoryName"), "inner") \
       .withColumn("ProductID", monotonically_increasing_id()) \
       .withColumn("Description", col("Product Name")) \
       .select("ProductID", "ProductCategoryID", col("Product Name").alias("ProductName"), "Description")

   # DimGeography
   dim_geography = df_orders.select("Customer Country", "Customer Region", "Customer Market").distinct() \
       .withColumn("GeographyID", monotonically_increasing_id()) \
       .select("GeographyID", col("Customer Country").alias("Country"), col("Customer Region").alias("Region"), col("Customer Market").alias("Market"))

   # DimCustomer
   dim_customer = df_orders.select(col("Customer ID").alias("CustomerDataID"), "Customer Country").distinct() \
       .join(dim_geography, col("Customer Country") == col("Country"), "inner") \
       .withColumn("CustomerID", monotonically_increasing_id()) \
       .withColumn("CustomerName", lit("")) \
       .select("CustomerID", "CustomerDataID", "CustomerName", "GeographyID")

   # DimDate (from min/max Order Date)
   min_date = df_orders.agg({"Order Date": "min"}).collect()[0][0]
   max_date = df_orders.agg({"Order Date": "max"}).collect()[0][0]
   date_range = spark.range(0, (max_date - min_date).days + 1) \
       .withColumn("Date", to_date(lit(min_date)) + col("id")) \
       .withColumn("DateKey", date_format(col("Date"), "yyyyMMdd").cast(IntegerType())) \
       .withColumn("Year", year(col("Date"))) \
       .withColumn("Month", month(col("Date"))) \
       .withColumn("Day", dayofmonth(col("Date"))) \
       .drop("id")

   # FactOrders
   fact_orders = df_orders \
       .join(dim_product, col("Product Name") == col("ProductName"), "inner") \
       .join(dim_customer, col("Customer ID") == col("CustomerDataID"), "inner") \
       .join(date_range.alias("order_date"), col("Order Date") == col("order_date.Date"), "inner") \
       .join(date_range.alias("shipment_date"), col("Shipment Date") == col("shipment_date.Date"), "inner") \
       .withColumn("OrderID", monotonically_increasing_id()) \
       .select("OrderID", col("Order ID").alias("OrderNumber"), col("Order Item ID").alias("OrderItemID"),
               col("order_date.DateKey").alias("OrderDateKey"), col("Order DateTime").alias("OrderDate"),
               "Order Quantity", "ProductID", "CustomerID", "Gross Sales", "Discount %", "Profit",
               "Net Sales", "Unit Price", col("Delay Shipment").alias("Shipment Feature"))

   # FactShipment
   fact_shipment = df_orders \
       .join(fact_orders, col("Order ID") == col("OrderNumber"), "inner") \
       .join(date_range.alias("shipment_date"), col("Shipment Date") == col("shipment_date.Date"), "inner") \
       .withColumn("ShipmentID", monotonically_increasing_id()) \
       .select("ShipmentID", "OrderID", col("shipment_date.DateKey").alias("ShipmentDateKey"), "Shipment Mode")

   # DimFulfillment
   dim_fulfillment = df_fulfillment \
       .join(dim_product, col("Product Name") == col("ProductName"), "inner") \
       .withColumn("FulfillmentID", monotonically_increasing_id()) \
       .select("FulfillmentID", "ProductID", col("Product Name"), col("Warehouse Order Fulfillment (days)"))

   # FactInventory (with Storage Cost KPI)
   fact_inventory = df_inventory \
       .withColumn("Storage Cost", col("Inventory Cost Per Unit") * col("Warehouse Inventory")) \
       .join(dim_product, col("Product Name") == col("ProductName"), "inner") \
       .withColumn("InventoryID", monotonically_increasing_id()) \
       .select("InventoryID", "ProductID", "Year Month", "Warehouse Inventory", "Inventory Cost Per Unit", "Storage Cost")

   # FactMonthlyOrders (aggregated)
   fact_monthly_orders = df_orders.groupBy("Order YearMonth") \
       .agg(sum("Net Sales").alias("Total Net Sales"), sum("Profit").alias("Total Profit"),
            count("Order ID").alias("Total Orders"),
            sum(when(col("Delay Shipment") == "Late", 1).otherwise(0)).alias("Late Orders")) \
       .withColumn("Profit Margin", col("Total Profit") / col("Total Net Sales")) \
       .withColumn("Late Shipment Rate", col("Late Orders") / col("Total Orders")) \
       .withColumn("MonthlyOrdersID", monotonically_increasing_id())

   # Write to Gold
   dim_product_dept.write.format("delta").mode("overwrite").save(gold_path + "DimProductDepartment")
   dim_product_category.write.format("delta").mode("overwrite").save(gold_path + "DimProductCategory")
   dim_product.write.format("delta").mode("overwrite").save(gold_path + "DimProduct")
   dim_geography.write.format("delta").mode("overwrite").save(gold_path + "DimGeography")
   dim_customer.write.format("delta").mode("overwrite").save(gold_path + "DimCustomer")
   date_range.write.format("delta").mode("overwrite").save(gold_path + "DimDate")
   fact_orders.write.format("delta").mode("overwrite").save(gold_path + "FactOrders")
   fact_shipment.write.format("delta").mode("overwrite").save(gold_path + "FactShipment")
   dim_fulfillment.write.format("delta").mode("overwrite").save(gold_path + "DimFulfillment")
   fact_inventory.write.format("delta").mode("overwrite").save(gold_path + "FactInventory")
   fact_monthly_orders.write.format("delta").mode("overwrite").save(gold_path + "FactMonthlyOrders")
   ```

3. **Test**: Run. Check gold container.

---

## **Section 7: ADF Pipeline for Gold Layer - PL_LOAD_GOLD_LAYER**
Similar to Silver.

1. **Pipeline PL_LOAD_GOLD_LAYER**:
   - New pipeline.
   - Add Notebook activity: `run_gold_notebook`, Notebook `nb_load_gold_layer`.

2. **Test**: Debug.

---

## **Section 8: Register Delta Tables in Unity Catalog**
In Databricks.

1. **Create Notebook nb_register_tables**.
2. **Paste and Run SQL Cells** (one per table):
   ```
   %sql
   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.DimProductDepartment
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/DimProductDepartment';

   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.DimProductCategory
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/DimProductCategory';

   -- Repeat for DimProduct, DimGeography, DimCustomer, DimDate, FactOrders, FactShipment, DimFulfillment, FactInventory, FactMonthlyOrders
   ```

3. **Verify Lineage**: Catalog UI > Select table > Lineage tab (shows dependencies if queries run).

---

## **Section 9: Master Pipeline, Scheduling, and Notifications - PL_LOAD_SUPPLYCHAIN_DATA_MASTER**
In ADF Studio.

1. **Create Master Pipeline**:
   - New > Name `PL_LOAD_SUPPLYCHAIN_DATA_MASTER`.
   - Activities > General > Execute Pipeline.
     - Add three: `exec_bronze` (invoke PL_COPY_RAW_FILES), `exec_silver` (PL_LOAD_SILVER_LAYER), `exec_gold` (PL_LOAD_GOLD_LAYER).
     - Connect with success dependencies (drag green arrow).

2. **Add Notifications**:
   - After each Execute, add Web activity.
     - Name: `notify_success`.
     - URL: Paste Logic App HTTP URL.
     - Method: POST.
     - Body: `{"message": "Pipeline @{activity('prev_activity').output.status} succeeded", "email": "your@email.com"}` (use expression builder for dynamic).
   - For failure: Add another Web on red arrow: Body with "failed".

3. **Create Trigger trg_daily_load**:
   - Triggers > New.
   - Name `trg_daily_load`.
   - Type Schedule.
   - Start date: Today.
   - Recurrence: Every 1 day.
   - Time: 12:05 (UTC—adjust if GUI shows local; confirm it's UTC).
   - Activated: Yes.
   - Associate with `PL_LOAD_SUPPLYCHAIN_DATA_MASTER`.

4. **Test**: Publish > Trigger now. Check emails.

---

## **Section 10: Create Power BI Dashboards**
Download Power BI Desktop (powerbi.microsoft.com).

1. **Connect to Databricks**:
   - Get data > Azure > Azure Databricks.
   - Server: Databricks workspace URL (e.g., https://eastus.azuredatabricks.net).
   - HTTP path: From cluster > Advanced options > JDBC/ODBC > Copy HTTP Path.
   - Database: `supply_chain_catalog.supply_chain_schema`.
   - Import mode.
   - Select all tables.

2. **Build Visuals (Answer 2+ Questions per Section)**:
   - **Business Performance Dashboard** (for Sales Manager):
     - Card: Total Net Sales = SUM(FactOrders[Net Sales]), Profit = SUM(Profit), Profit Margin = [Total Profit] / [Total Net Sales].
     - Line chart: Net Sales over time (Order YearMonth on X, Net Sales on Y).
     - Bar: Product departments by net sales (ProductDepartmentName, SUM(Net Sales)).
   - **Customer Dashboard**:
     - Map: Customer distribution (Country, COUNT(CustomerID)).
     - Line: Customers over time (Order YearMonth, COUNT DISTINCT CustomerID).
   - **Product Dashboard**:
     - Bar: Top categories/names by orders (ProductCategoryName, COUNT(OrderID)).
     - Bar: Most profitable (ProductName, SUM(Profit)).
   - **Inventory Dashboard** (for Inventory Manager):
     - Bar: Departments by storage cost (ProductDepartmentName, SUM(Storage Cost)).
     - Line: Inventory changes over time (Year Month, SUM(Warehouse Inventory)).
     - Card: Avg fulfillment = AVG(DimFulfillment[Warehouse Order Fulfillment (days)]).
   - **Shipment Dashboard** (for Shipping Manager):
     - Pie: Shipment modes (Shipment Mode, COUNT).
     - Bar: Late rate by department (ProductDepartmentName, Late Shipment Rate measure = DIVIDE(COUNTROWS(FILTER(FactOrders, [Shipment Feature] = "Late")), COUNTROWS(FactOrders))).
     - Line: Late rate over time (Order YearMonth, Late Shipment Rate).

3. **Publish**: File > Publish to Power BI Service > Workspace your choice.
   - Add slicers for interactivity.

Take screenshots of dashboards.

---

## **Section 11: Final Documentation and Submission**
1. **Update PPT**: Add slides with screenshots of each step (RG creation, pipelines, notebooks runs, dashboards). Include name/ID in Azure screenshots.
2. **Export Notebooks**: In Databricks > Notebooks > Export > DBC Archive > Download.
3. **ZIP File**: Download GitHub repo as ZIP. Add PPT, DBC file, extra screenshots inside.
4. **Submit**: Upload ZIP (ensure <100MB).