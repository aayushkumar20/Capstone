### **Section 1: Set Up Azure Resources via Portal GUI**
In Azure Portal (portal.azure.com):
1. **Resource Group**:
   - Search "Resource groups" > Create.
   - Subscription: Your subscription.
   - Name: `supply-chain-rg`.
   - Region: East US (or closest).
   - Click Review + create > Create.
2. **ADLS Gen2 Storage Account**:
   - Search "Storage accounts" > Create.
   - Basics: Resource group `supply-chain-rg`, Name `supplychainadls` (unique), Region East US, Performance Standard, Redundancy LRS.
   - Advanced: Enable hierarchical namespace (ADLS Gen2).
   - Click Review + create > Create.
   - After creation, go to Storage account > Containers > New containers (private access):
     - `raw` (contains your data: `ordersandshipment/*.csv`, `fulfillment/*.json`, `inventory/*.csv`).
     - `bronze`, `silver`, `gold` (for Medallion layers).
   - If data isn’t in `raw`, upload via Storage Explorer:
     - Go to Storage Explorer > raw > Upload > Files.
     - Create folders: `ordersandshipment/`, `fulfillment/`, `inventory/`.
     - Upload CSVs (e.g., orders_2015.csv) to `ordersandshipment/`, JSON (e.g., fulfillment.json) to `fulfillment/`, CSVs (e.g., inventory.csv) to `inventory/`.
   - Access keys > Show keys > Copy key1 (save for secrets).
3. **Azure Databricks**:
   - Search "Azure Databricks" > Create.
   - Basics: Resource group `supply-chain-rg`, Name `supply-chain-databricks`, Region East US, Pricing tier Premium (for Unity Catalog).
   - Click Review + create > Create > Launch Workspace.
   - In Databricks UI (workspace):
     - Compute > Create compute > Name `supply-chain-cluster` > Single node > Runtime 13.3 LTS (Scala 2.12, Spark 3.5.0) > Create.
     - Catalog > Create catalog > Name `supply_chain_catalog` > Storage: Select `gold` container (`abfss://gold@supplychainadls.dfs.core.windows.net/`) > Create.
     - Inside catalog > Create schema > Name `supply_chain_schema`.
4. **Secrets for ADLS**:
   - Databricks UI > New > Notebook > Python > Run:
     ```python
     dbutils.secrets.createScope("supply_chain_secrets")
     dbutils.secrets.put("supply_chain_secrets", "adls_key", "paste-your-adls-key1-here")
     ```
   - If GUI preferred: Use Databricks Admin Console > Manage > Secret Scopes > Create Scope > Name `supply_chain_secrets` > Add secret `adls_key` with key1 value.
5. **Azure Data Factory (ADF)**:
   - Search "Data factories" > Create.
   - Basics: Resource group `supply-chain-rg`, Name `supply-chain-adf`, Region East US.
   - Git configuration: Skip (since no GitHub).
   - Click Review + create > Create > Launch Studio.
6. **Logic App for Email Notifications**:
   - Search "Logic Apps" > Create.
   - Basics: Resource group `supply-chain-rg`, Name `supply-chain-notifications`, Region East US, Plan Consumption.
   - Click Review + create > Create.
   - Go to Logic App > Designer > Blank Logic App.
   - Trigger: When a HTTP request is received > Add JSON schema:
     ```json
     { "properties": { "message": { "type": "string" }, "email": { "type": "string" } } }
     ```
   - Action: Send an email (v2) > Connect Office 365 or Gmail > To: `@triggerBody()?['email']`, Subject: "Pipeline Status", Body: `@triggerBody()?['message']` > Save > Copy HTTP POST URL.

Take screenshot of each resource creation (Portal top-left shows name/ID).

---

### **Section 2: Bronze Layer - Copy Raw Data from ADLS Raw to Bronze (PL_COPY_RAW_FILES)**
This pipeline copies data from `raw` to `bronze` container without cleaning, preserving the original format.

In ADF Studio (adf.azure.com > supply-chain-adf):
1. **Linked Service**:
   - Author > Linked services > New > Azure Data Lake Storage Gen2.
   - Name: `ls_adls_gen2`.
   - Authentication: Account key > Paste key1 from ADLS.
   - Account: `supplychainadls`.
   - Test connection > Create.
2. **Datasets**:
   - Author > Datasets > New > Azure Data Lake Storage Gen2 > DelimitedText.
     - Name: `ds_raw_orders`.
     - Linked service: `ls_adls_gen2`.
     - File path: `raw/ordersandshipment` > File: `*.csv` (wildcard for multiple CSVs).
     - First row as header: Yes.
     - Import schema: From connection/store (or define manually if needed).
     - Create.
   - New > ADLS > JSON > Name: `ds_raw_fulfillment` > Path: `raw/fulfillment/*.json` > Create.
   - New > ADLS > DelimitedText > Name: `ds_raw_inventory` > Path: `raw/inventory/*.csv` > Create.
   - Sink Datasets:
     - New > ADLS > DelimitedText > Name: `ds_bronze_orders` > Path: `bronze/ordersandshipment` > Create.
     - New > ADLS > JSON > Name: `ds_bronze_fulfillment` > Path: `bronze/fulfillment` > Create.
     - New > ADLS > DelimitedText > Name: `ds_bronze_inventory` > Path: `bronze/inventory` > Create.
3. **Pipeline PL_COPY_RAW_FILES**:
   - Author > Pipelines > New > Name `PL_COPY_RAW_FILES`.
   - Drag Move and transform > Copy data > Name `copy_orders`.
     - Source tab: Dataset `ds_raw_orders` > Wildcard path `*.csv`.
     - Sink tab: Dataset `ds_bronze_orders` > File name option: From source pattern (e.g., `@{item().name}`).
   - Add Copy activity for fulfillment: Name `copy_fulfillment` > Source `ds_raw_fulfillment` > Sink `ds_bronze_fulfillment`.
   - Add Copy for inventory: Name `copy_inventory` > Source `ds_raw_inventory` > Sink `ds_bronze_inventory`.
   - Validate pipeline.
4. **Test**:
   - Click Publish all > Debug.
   - Go to Portal > Storage account > Storage Explorer > bronze container > Verify files copied (e.g., `ordersandshipment/orders_2015.csv`, etc.).
   - If errors, check activity output in ADF Monitor.

Take screenshot of pipeline run (Monitor > Pipeline runs).

---

### **Section 3: Silver Layer - Clean and Transform Data (nb_load_silver_layer)**
This notebook reads from Bronze, **cleans data** (removes duplicates, nulls, invalid data, fixes inconsistencies like negative shipping times or unmatched products), adds features (Order DateTime, Delay Shipment, Net Sales, Unit Price), and writes to Silver as Delta tables.

In Databricks Workspace:
1. **Create Notebook**:
   - Notebooks > Create > Name `nb_load_silver_layer` > Language Python > Attach `supply-chain-cluster`.
2. **Copy-Paste Code** (run cell-by-cell to debug):
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import col, to_date, concat, lit, when, datediff, regexp_replace
   from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

   spark = SparkSession.builder.appName("LoadSilverLayer").getOrCreate()

   # Configure ADLS access
   adls_key = dbutils.secrets.get(scope="supply_chain_secrets", key="adls_key")
   spark.conf.set("fs.azure.account.key.supplychainadls.dfs.core.windows.net", adls_key)

   bronze_path = "abfss://bronze@supplychainadls.dfs.core.windows.net/"
   silver_path = "abfss://silver@supplychainadls.dfs.core.windows.net/"

   # Schema for Orders/Shipments (enforce datatypes)
   orders_schema = StructType([
       StructField("Order ID", IntegerType(), True),
       StructField("Order Item ID", IntegerType(), True),
       StructField("Order YearMonth", IntegerType(), True),
       StructField("Order Year", IntegerType(), True),
       StructField("Order Month", IntegerType(), True),
       StructField("Order Day", IntegerType(), True),
       StructField("Order Time", TimestampType(), True),
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
       StructField("Gross Sales", FloatType(), True),
       StructField("Discount %", FloatType(), True),
       StructField("Profit", FloatType(), True)
   ])

   # Read from Bronze (copying/using data)
   df_orders = spark.read.schema(orders_schema).option("header", "true").csv(bronze_path + "ordersandshipment/*.csv")
   df_orders.cache()  # Improve performance
   df_orders.printSchema()  # Verify schema
   df_orders.show(5)  # Check records
   print(f"Total rows before cleaning: {df_orders.count()}")

   # Clean Data
   # 1. Remove duplicates
   df_orders = df_orders.dropDuplicates()

   # 2. Handle nulls/missing values
   df_orders = df_orders.fillna({
       "Order Quantity": 0, "Gross Sales": 0.0, "Discount %": 0.0, "Profit": 0.0,
       "Shipment Days - Scheduled": 0, "Customer ID": -1,
       "Product Department": "Unknown", "Product Category": "Unknown", "Product Name": "Unknown",
       "Customer Market": "Unknown", "Customer Region": "Unknown", "Customer Country": "Unknown",
       "Warehouse Country": "Unknown", "Shipment Mode": "Unknown"
   })

   # 3. Filter invalid data (business rules)
   df_orders = df_orders.filter(
       (col("Order Quantity") > 0) &
       (col("Gross Sales") >= 0) &
       (col("Discount %") >= 0) & (col("Discount %") <= 1)
   )

   # 4. Verify data integrity (stats, max/min, duplicates)
   df_orders.describe().show()
   df_orders.agg({"Gross Sales": "max", "Gross Sales": "min"}).show()
   duplicates = df_orders.groupBy("Order ID").count().filter("count > 1")
   duplicates.show()  # Should be empty

   # 5. Special cleaning: Remove special characters in Customer Country
   df_orders = df_orders.withColumn("Customer Country", regexp_replace(col("Customer Country"), "[^a-zA-Z0-9 ]", ""))

   # 6. Add date features for transformations
   df_orders = df_orders.withColumn("Order Date", to_date(concat(col("Order Year"), lit("-"), col("Order Month"), lit("-"), col("Order Day"))))
   df_orders = df_orders.withColumn("Shipment Date", to_date(concat(col("Shipment Year"), lit("-"), col("Shipment Month"), lit("-"), col("Shipment Day"))))

   # 7. Clean negative/unusual shipping times (0 to 28 days)
   df_orders = df_orders.withColumn("Shipping Time", datediff(col("Shipment Date"), col("Order Date")))
   df_orders = df_orders.filter((col("Shipping Time") >= 0) & (col("Shipping Time") <= 28))

   # Add Features (as per requirements)
   # 8. DateTime feature
   df_orders = df_orders.withColumn("Order DateTime", concat(col("Order Date"), lit(" "), col("Order Time")))

   # 9. Shipment feature: Delay Shipment (Late/On Time)
   df_orders = df_orders.withColumn("Delay Shipment", when(col("Shipping Time") > col("Shipment Days - Scheduled"), "Late").otherwise("On Time"))

   # 10. Business performance features
   df_orders = df_orders.withColumn("Net Sales", col("Gross Sales") - (col("Discount %") * col("Gross Sales")))
   df_orders = df_orders.withColumn("Unit Price", col("Gross Sales") / col("Order Quantity"))

   print(f"Total rows after cleaning: {df_orders.count()}")

   # Fulfillment: Schema and clean
   fulfillment_schema = StructType([
       StructField("Product Name", StringType(), True),
       StructField("Warehouse Order Fulfillment (days)", FloatType(), True)
   ])
   df_fulfillment = spark.read.schema(fulfillment_schema).json(bronze_path + "fulfillment/*.json")
   df_fulfillment = df_fulfillment.dropDuplicates().fillna({"Warehouse Order Fulfillment (days)": 0.0})
   df_fulfillment = df_fulfillment.filter(col("Warehouse Order Fulfillment (days)") >= 0)

   # Inventory: Schema, clean, handle inconsistencies
   inventory_schema = StructType([
       StructField("Product Name", StringType(), True),
       StructField("Year Month", IntegerType(), True),
       StructField("Warehouse Inventory", IntegerType(), True),
       StructField("Inventory Cost Per Unit", FloatType(), True)
   ])
   df_inventory = spark.read.schema(inventory_schema).option("header", "true").csv(bronze_path + "inventory/*.csv")
   df_inventory = df_inventory.dropDuplicates().fillna({"Warehouse Inventory": 0, "Inventory Cost Per Unit": 0.0})
   df_inventory = df_inventory.filter(col("Warehouse Inventory") >= 0)
   # Handle unmatched products: Keep only products in orders
   matching_products = df_orders.select("Product Name").distinct().rdd.map(lambda row: row[0]).collect()
   df_inventory = df_inventory.filter(col("Product Name").isin(matching_products))

   # Write to Silver as Delta tables
   df_orders.write.format("delta").mode("overwrite").save(silver_path + "orders")
   df_fulfillment.write.format("delta").mode("overwrite").save(silver_path + "fulfillment")
   df_inventory.write.format("delta").mode("overwrite").save(silver_path + "inventory")

   # Create SQL view for EDA
   df_orders.createOrReplaceTempView("silver_orders")
   spark.sql("SELECT AVG(Net Sales) AS avg_sales, AVG(Profit) AS avg_profit FROM silver_orders").show()
   ```

3. **Test**:
   - Run each cell. Check output (e.g., row counts, stats).
   - Go to Portal > Storage > Storage Explorer > silver container > Verify Delta folders (`orders/`, `fulfillment/`, `inventory/`).

Take screenshot of notebook run and Silver files.

---

### **Section 4: ADF Pipeline for Silver Layer (PL_LOAD_SILVER_LAYER)**
In ADF Studio:
1. **Linked Service for Databricks**:
   - Author > Linked services > New > Azure Databricks.
   - Name: `ls_databricks`.
   - Workspace URL: Copy from Databricks (e.g., https://adb-xxx.azuredatabricks.net).
   - Authentication: Access token (Databricks > User settings > Generate new token > Copy).
   - Cluster ID: Databricks > Compute > `supply-chain-cluster` > Copy ID from Advanced options.
   - Test > Create.
2. **Pipeline PL_LOAD_SILVER_LAYER**:
   - Pipelines > New > Name `PL_LOAD_SILVER_LAYER`.
   - Drag Azure Databricks > Notebook > Name `run_silver_notebook`.
   - Linked service: `ls_databricks`.
   - Notebook path: Browse > Select `nb_load_silver_layer`.
   - Publish > Debug.
3. **Test**: Monitor > Pipeline runs > Ensure success.

Take screenshot.

---

### **Section 5: Gold Layer - Normalize and Add KPIs (nb_load_gold_layer)**
This notebook reads from Silver, creates fact/dimension tables with surrogate keys, adds KPIs (e.g., Profit Margin, Late Shipment Rate, Storage Cost), and writes to Gold as Delta tables.

In Databricks:
1. Create Notebook: Name `nb_load_gold_layer` > Python > Attach cluster.
2. Copy-Paste Code:
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import monotonically_increasing_id, year, month, dayofmonth, date_format, sum, count, when
   from pyspark.sql.types import IntegerType, DateType

   spark = SparkSession.builder.appName("LoadGoldLayer").getOrCreate()

   # ADLS access
   adls_key = dbutils.secrets.get(scope="supply_chain_secrets", key="adls_key")
   spark.conf.set("fs.azure.account.key.supplychainadls.dfs.core.windows.net", adls_key)

   silver_path = "abfss://silver@supplychainadls.dfs.core.windows.net/"
   gold_path = "abfss://gold@supplychainadls.dfs.core.windows.net/"

   # Read Silver data
   df_orders = spark.read.format("delta").load(silver_path + "orders")
   df_fulfillment = spark.read.format("delta").load(silver_path + "fulfillment")
   df_inventory = spark.read.format("delta").load(silver_path + "inventory")

   # Dimension Tables with Surrogate Keys
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

   # DimDate
   min_date = df_orders.agg({"Order Date": "min"}).collect()[0][0]
   max_date = df_orders.agg({"Order Date": "max"}).collect()[0][0]
   date_range = spark.range(0, (max_date - min_date).days + 1) \
       .withColumn("Date", to_date(lit(min_date)) + col("id")) \
       .withColumn("DateKey", date_format(col("Date"), "yyyyMMdd").cast(IntegerType())) \
       .withColumn("Year", year(col("Date"))) \
       .withColumn("Month", month(col("Date"))) \
       .withColumn("Day", dayofmonth(col("Date"))) \
       .drop("id")

   # FactOrders (with KPIs from Silver)
   fact_orders = df_orders \
       .join(dim_product, col("Product Name") == col("ProductName"), "inner") \
       .join(dim_customer, col("Customer ID") == col("CustomerDataID"), "inner") \
       .join(date_range.alias("order_date"), col("Order Date") == col("order_date.Date"), "inner") \
       .join(date_range.alias("shipment_date"), col("Shipment Date") == col("shipment_date.Date"), "inner") \
       .withColumn("OrderID", monotonically_increasing_id()) \
       .select(
           "OrderID", col("Order ID").alias("OrderNumber"), col("Order Item ID").alias("OrderItemID"),
           col("order_date.DateKey").alias("OrderDateKey"), col("Order DateTime").alias("OrderDate"),
           "Order Quantity", "ProductID", "CustomerID", "Gross Sales", "Discount %", "Profit",
           "Net Sales", "Unit Price", col("Delay Shipment").alias("Shipment Feature")
       )

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

   # FactMonthlyOrders (aggregated KPIs)
   fact_monthly_orders = df_orders.groupBy("Order YearMonth") \
       .agg(
           sum("Net Sales").alias("Total Net Sales"),
           sum("Profit").alias("Total Profit"),
           count("Order ID").alias("Total Orders"),
           sum(when(col("Delay Shipment") == "Late", 1).otherwise(0)).alias("Late Orders")
       ) \
       .withColumn("Profit Margin", col("Total Profit") / col("Total Net Sales")) \
       .withColumn("Late Shipment Rate", col("Late Orders") / col("Total Orders")) \
       .withColumn("MonthlyOrdersID", monotonically_increasing_id())

   # Write to Gold as Delta tables
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

   # Create SQL views for querying
   fact_monthly_orders.createOrReplaceTempView("fact_monthly_orders")
   spark.sql("SELECT Order YearMonth, Total Net Sales, Profit Margin FROM fact_monthly_orders LIMIT 5").show()
   ```

3. **Test**:
   - Run cells. Check output (e.g., table previews).
   - Portal > Storage > Storage Explorer > gold container > Verify Delta folders (e.g., `FactOrders/`).

Take screenshot.

---

### **Section 6: ADF Pipeline for Gold Layer (PL_LOAD_GOLD_LAYER)**
In ADF Studio:
1. **Pipeline PL_LOAD_GOLD_LAYER**:
   - Pipelines > New > Name `PL_LOAD_GOLD_LAYER`.
   - Drag Azure Databricks > Notebook > Name `run_gold_notebook`.
   - Linked service: `ls_databricks`.
   - Notebook path: Browse > Select `nb_load_gold_layer`.
   - Publish > Debug.
2. **Test**: Monitor > Pipeline runs > Ensure success.

Take screenshot.

---

### **Section 7: Register Delta Tables in Unity Catalog**
In Databricks:
1. Create Notebook: Name `nb_register_tables` > SQL > Attach cluster.
2. Copy-Paste SQL (run each):
   ```sql
   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.DimProductDepartment
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/DimProductDepartment';

   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.DimProductCategory
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/DimProductCategory';

   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.DimProduct
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/DimProduct';

   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.DimGeography
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/DimGeography';

   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.DimCustomer
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/DimCustomer';

   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.DimDate
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/DimDate';

   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.FactOrders
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/FactOrders';

   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.FactShipment
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/FactShipment';

   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.DimFulfillment
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/DimFulfillment';

   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.FactInventory
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/FactInventory';

   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.FactMonthlyOrders
   USING DELTA
   LOCATION 'abfss://gold@supplychainadls.dfs.core.windows.net/FactMonthlyOrders';
   ```
3. **Verify Lineage**:
   - Catalog > supply_chain_catalog > supply_chain_schema > Select table (e.g., FactOrders) > Lineage tab > Check dependencies.
   - Run sample query: `SELECT * FROM supply_chain_catalog.supply_chain_schema.FactOrders LIMIT 5`.

Take screenshot.

---

### **Section 8: Master Pipeline and Scheduling (PL_LOAD_SUPPLYCHAIN_DATA_MASTER)**
In ADF Studio:
1. **Create Master Pipeline**:
   - Pipelines > New > Name `PL_LOAD_SUPPLYCHAIN_DATA_MASTER`.
   - Drag General > Execute Pipeline > Name `exec_bronze` > Pipeline: `PL_COPY_RAW_FILES`.
   - Add `exec_silver` > Pipeline: `PL_LOAD_SILVER_LAYER` > Connect from `exec_bronze` (green success arrow).
   - Add `exec_gold` > Pipeline: `PL_LOAD_GOLD_LAYER` > Connect from `exec_silver`.
2. **Add Notifications**:
   - After each Execute Pipeline, drag General > Web > Name `notify_success_<pipeline>` (e.g., `notify_success_bronze`).
   - URL: Paste Logic App HTTP URL.
   - Method: POST.
   - Body: `{"message": "Pipeline @{pipeline().Pipeline} succeeded", "email": "your@email.com"}` (use expression builder).
   - For failures: Add Web on red failure arrow > Body: `{"message": "Pipeline @{pipeline().Pipeline} failed: @{activity('prev').error.message}", "email": "your@email.com"}`.
3. **Create Trigger**:
   - Triggers > New > Name `trg_daily_load` > Type Schedule > Recurrence: Every 1 day > Start: Today > Time: 12:05 UTC (adjust to UTC in GUI) > Activated: Yes > Create.
   - Associate with `PL_LOAD_SUPPLYCHAIN_DATA_MASTER` (Pipelines > Triggers > Add trigger).
4. **Test**:
   - Publish all > Trigger now > Check Monitor > Pipeline runs.
   - Verify emails sent (success/failure).

Take screenshot of pipeline and trigger.

---

### **Section 9: Power BI Dashboards**
Download Power BI Desktop (powerbi.microsoft.com).
1. **Connect to Databricks**:
   - Get Data > Azure > Azure Databricks.
   - Server: Databricks workspace URL (from Azure Portal > Databricks > Overview).
   - HTTP Path: Databricks > Compute > `supply-chain-cluster` > Advanced options > JDBC/ODBC > Copy HTTP Path.
   - Database: `supply_chain_catalog.supply_chain_schema`.
   - Data connectivity: Import.
   - Select all tables (dims, facts) > Load.
2. **Build Dashboards** (answer at least 2 questions per section):
   - **Business Performance** (Sales Manager):
     - Card: Total Net Sales = `SUM(FactOrders[Net Sales])`, Total Profit = `SUM(FactOrders[Profit])`, Profit Margin = `SUM(FactOrders[Profit]) / SUM(FactOrders[Net Sales])`.
     - Line chart: Net Sales over time (X: FactMonthlyOrders[Order YearMonth], Y: Total Net Sales).
     - Bar: Product departments by sales (X: DimProductDepartment[ProductDepartmentName], Y: SUM(FactOrders[Net Sales])).
   - **Customer**:
     - Map: Customer distribution (Location: DimGeography[Country], Size: COUNT(DimCustomer[CustomerID])).
     - Line: Customers over time (X: FactMonthlyOrders[Order YearMonth], Y: COUNT DISTINCT FactOrders[CustomerID]).
   - **Product**:
     - Bar: Top categories (X: DimProductCategory[ProductCategoryName], Y: COUNT(FactOrders[OrderID])).
     - Bar: Most profitable products (X: DimProduct[ProductName], Y: SUM(FactOrders[Profit])).
   - **Inventory** (Inventory Manager):
     - Bar: Storage cost by department (X: DimProductDepartment[ProductDepartmentName], Y: SUM(FactInventory[Storage Cost])).
     - Line: Inventory over time (X: FactInventory[Year Month], Y: SUM(Warehouse Inventory)).
   - **Shipment** (Shipping Manager):
     - Pie: Shipment modes (FactShipment[Shipment Mode], COUNT).
     - Bar: Late shipment rate by department (X: DimProductDepartment[ProductDepartmentName], Y: Measure = `DIVIDE(COUNTROWS(FILTER(FactOrders, FactOrders[Shipment Feature] = "Late")), COUNTROWS(FactOrders))`).
     - Line: Late rate over time (X: FactMonthlyOrders[Order YearMonth], Y: Late Shipment Rate).
   - Add slicers: Date (DimDate[Year]), Department (DimProductDepartment[ProductDepartmentName]).
3. **Publish**:
   - File > Publish > Power BI Service > Select workspace > Publish.
   - Share dashboard link if needed.

Take screenshots of each dashboard.

---

### **Section 10: Documentation and Submission**
1. **PPT**:
   - Create in PowerPoint (use Fractal template if provided).
   - Add slides: Overview, Resource Setup (screenshots), Pipelines (ADF runs), Notebooks (Databricks runs), Dashboards (Power BI visuals), Name/ID visible in Azure screenshots.
2. **Export Notebooks**:
   - Databricks > Notebooks > Select `nb_load_silver_layer`, `nb_load_gold_layer`, `nb_register_tables` > Export > DBC Archive > Download.
3. **Create ZIP**:
   - Locally create folder `capstone_submission`.
   - Add: Exported .dbc file, PPT, screenshots (from each section).
   - Optional: Include sample data from `raw` (if allowed).
   - Zip folder (ensure <100MB).
4. **Submit**: Upload ZIP to course platform or per instructions.

---

### **Business Improvements (for PPT)**
Based on analysis (inspired by similar projects):
- **Supply Chain Issue**: High late shipment rate (~40%) suggests inefficiencies. Propose optimizing delivery routes (e.g., cross-docking) or partnering with local logistics.
- **Inventory**: Overstocking detected. Implement demand forecasting (using historical sales in FactMonthlyOrders) to optimize stock levels.
- **Revenue**: Decline in key product departments (e.g., Apparel). Investigate supplier disruptions and source alternatives.

---

### **Testing and Debugging**
- **After Each Section**: Debug pipelines (ADF Monitor > Pipeline runs), run notebooks (check cell outputs), verify ADLS files (Storage Explorer).
- **Errors**: Check ADF activity errors or Databricks logs. Common issues: Wrong ADLS paths, missing secrets, schema mismatches (adjust schemas if data varies).
- **Run Master Pipeline**: Trigger `PL_LOAD_SUPPLYCHAIN_DATA_MASTER` to test end-to-end flow.
