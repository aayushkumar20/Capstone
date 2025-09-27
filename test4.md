### **Section 1: Set Up Azure Resources via Portal GUI**
In Azure Portal (portal.azure.com):
1. **Resource Group**:
   - Search "Resource groups" > Create.
   - Name: `supply-chain-rg`.
   - Region: East US.
   - Review + create > Create.
2. **ADLS Gen2 Storage Account**:
   - Search "Storage accounts" > Create.
   - Basics: Resource group `supply-chain-rg`, Name `supplychainadls` (unique), Region East US, Performance Standard, Redundancy LRS.
   - Advanced: Enable hierarchical namespace.
   - Review + create > Create.
   - Containers > New:
     - `source` (contains `fulfillment/`, `ordersandshipment/`, `inventory/`).
     - `raw` (for copied data).
     - `tables` (for Medallion: `tables/bronze/`, `tables/silver/`, `tables/gold/`).
   - If data not in `source`, upload via Storage Explorer:
     - Storage Explorer > source > Upload > Files.
     - Folders: `fulfillment/` (e.g., `fulfillment.json`), `ordersandshipment/` (e.g., `orders_2015.csv`, `orders_2016.csv`), `inventory/` (e.g., `inventory.csv`).
   - Access keys > Show keys > Copy key1.
3. **Azure Databricks**:
   - Search "Azure Databricks" > Create.
   - Resource group `supply-chain-rg`, Name `supply-chain-databricks`, Region East US, Pricing tier Premium.
   - Review + create > Create > Launch Workspace.
   - Databricks UI:
     - Compute > Create compute > Name `supply-chain-cluster` > Single node > Runtime 13.3 LTS > Create.
     - Catalog > Create catalog > Name `supply_chain_catalog` > Storage: `tables/gold/` > Create.
     - Inside catalog > Create schema > Name `supply_chain_schema`.
4. **Secrets**:
   - Databricks > New Notebook > Python > Run:
     ```python
     dbutils.secrets.createScope("supply_chain_secrets")
     dbutils.secrets.put("supply_chain_secrets", "adls_key", "paste-your-adls-key1-here")
     ```
5. **Azure Data Factory**:
   - Search "Data factories" > Create.
   - Resource group `supply-chain-rg`, Name `supply-chain-adf`, Region East US.
   - Skip Git integration.
   - Review + create > Create > Launch Studio.
6. **Logic App**:
   - Search "Logic Apps" > Create.
   - Resource group `supply-chain-rg`, Name `supply-chain-notifications`, Region East US, Plan Consumption.
   - Review + create > Create.
   - Logic App > Designer > Blank Logic App > Trigger: When a HTTP request is received > JSON schema:
     ```json
     { "properties": { "message": { "type": "string" }, "email": { "type": "string" } } }
     ```
   - Action: Send an email (v2) > Connect Office 365/Gmail > To: `@triggerBody()?['email']`, Subject: "Pipeline Status", Body: `@triggerBody()?['message']` > Save > Copy HTTP POST URL.

Take screenshot of each (name/ID in Azure UI).

---

### **Section 2: Bronze Layer - Copy Data from Source to Raw with File Check and Delete (PL_COPY_RAW_FILES)**
This pipeline checks if all files exist in `source`, copies to `raw` with same folder structure, and deletes from `source` on success. No nested pipelines.

In ADF Studio (adf.azure.com > supply-chain-adf):
1. **Linked Service**:
   - Author > Linked services > New > Azure Data Lake Storage Gen2 > Name `ls_adls_gen2` > Account key > Paste key1 > Account `supplychainadls` > Test > Create.
2. **Datasets**:
   - Source Datasets:
     - New > ADLS > DelimitedText > Name `ds_source_orders` > Linked `ls_adls_gen2` > Path `source/ordersandshipment/*.csv` > Header yes > Create.
     - New > ADLS > JSON > Name `ds_source_fulfillment` > Path `source/fulfillment/*.json` > Create.
     - New > ADLS > DelimitedText > Name `ds_source_inventory` > Path `source/inventory/*.csv` > Header yes > Create.
   - Sink Datasets:
     - New > ADLS > DelimitedText > Name `ds_raw_orders` > Path `raw/ordersandshipment/` > Create.
     - New > ADLS > JSON > Name `ds_raw_fulfillment` > Path `raw/fulfillment/` > Create.
     - New > ADLS > DelimitedText > Name `ds_raw_inventory` > Path `raw/inventory/` > Create.
3. **Pipeline PL_COPY_RAW_FILES**:
   - Pipelines > New > Name `PL_COPY_RAW_FILES`.
   - **Check File Existence**:
     - Drag General > Get Metadata > Name `check_orders`.
     - Dataset: `ds_source_orders` > Field list: Add `Child Items` (lists files in folder).
     - Drag If Condition > Name `if_orders_exist` > Expression: `@greater(length(activity('check_orders').output.childItems), 0)`.
     - True activities:
       - Copy data > Name `copy_orders` > Source: `ds_source_orders` (Wildcard `*.csv`) > Sink: `ds_raw_orders` > File name from source.
       - Delete > Name `delete_orders` > Dataset: `ds_source_orders` > Delete type: Delete files > Wildcard `*.csv`.
     - False activities: Web > Name `notify_no_orders` > URL: Logic App URL > Method: POST > Body: `{"message": "No orders files found in source/ordersandshipment/", "email": "your@email.com"}`.
   - Repeat for fulfillment:
     - Get Metadata > Name `check_fulfillment` > Dataset `ds_source_fulfillment` > Child Items.
     - If Condition > Name `if_fulfillment_exist` > Expression: `@greater(length(activity('check_fulfillment').output.childItems), 0)`.
     - True: Copy `copy_fulfillment`, Delete `delete_fulfillment`.
     - False: Web `notify_no_fulfillment` > Body: `{"message": "No fulfillment files found", "email": "your@email.com"}`.
   - Repeat for inventory (same pattern).
   - Connect activities: `check_orders` > `if_orders_exist` > `check_fulfillment` > `if_fulfillment_exist` > `check_inventory` > `if_inventory_exist` (sequential, no nesting).
4. **Test**:
   - Publish > Debug.
   - Check: Portal > Storage > raw (files copied), source (files deleted).
   - Verify email notifications if files missing.

Take screenshot (pipeline run in Monitor).

---

### **Section 3: Silver Layer - Clean, Transform, and UPSERT with Timestamps (nb_load_silver_layer)**
This notebook reads from `raw`, cleans data, adds timestamps (`created_at`, `updated_at`), implements **UPSERT** using Delta Merge (updates if existing records changed, inserts new), and writes to `tables/silver/` as Delta tables.

In Databricks Workspace:
1. Create Notebook: Notebooks > Create > Name `nb_load_silver_layer` > Python > Attach `supply-chain-cluster`.
2. Copy-Paste Code:
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import col, to_date, concat, lit, when, datediff, regexp_replace, current_timestamp
   from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
   from delta.tables import DeltaTable

   spark = SparkSession.builder.appName("LoadSilverLayer").getOrCreate()

   # ADLS access
   adls_key = dbutils.secrets.get(scope="supply_chain_secrets", key="adls_key")
   spark.conf.set("fs.azure.account.key.supplychainadls.dfs.core.windows.net", adls_key)

   raw_path = "abfss://raw@supplychainadls.dfs.core.windows.net/"
   silver_path = "abfss://tables@supplychainadls.dfs.core.windows.net/silver/"

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

   # Read from Raw
   df_orders = spark.read.schema(orders_schema).option("header", "true").csv(raw_path + "ordersandshipment/*.csv")
   df_orders = df_orders.cache()

   # Clean Data
   df_orders = df_orders.dropDuplicates()
   df_orders = df_orders.fillna({
       "Order Quantity": 0, "Gross Sales": 0.0, "Discount %": 0.0, "Profit": 0.0,
       "Shipment Days - Scheduled": 0, "Customer ID": -1,
       "Product Department": "Unknown", "Product Category": "Unknown", "Product Name": "Unknown",
       "Customer Market": "Unknown", "Customer Region": "Unknown", "Customer Country": "Unknown",
       "Warehouse Country": "Unknown", "Shipment Mode": "Unknown"
   })
   df_orders = df_orders.filter(
       (col("Order Quantity") > 0) &
       (col("Gross Sales") >= 0) &
       (col("Discount %") >= 0) & (col("Discount %") <= 1)
   )
   df_orders = df_orders.withColumn("Customer Country", regexp_replace(col("Customer Country"), "[^a-zA-Z0-9 ]", ""))
   df_orders = df_orders.withColumn("Order Date", to_date(concat(col("Order Year"), lit("-"), col("Order Month"), lit("-"), col("Order Day"))))
   df_orders = df_orders.withColumn("Shipment Date", to_date(concat(col("Shipment Year"), lit("-"), col("Shipment Month"), lit("-"), col("Shipment Day"))))
   df_orders = df_orders.withColumn("Shipping Time", datediff(col("Shipment Date"), col("Order Date")))
   df_orders = df_orders.filter((col("Shipping Time") >= 0) & (col("Shipping Time") <= 28))

   # Add Features
   df_orders = df_orders.withColumn("Order DateTime", concat(col("Order Date"), lit(" "), col("Order Time")))
   df_orders = df_orders.withColumn("Delay Shipment", when(col("Shipping Time") > col("Shipment Days - Scheduled"), "Late").otherwise("On Time"))
   df_orders = df_orders.withColumn("Net Sales", col("Gross Sales") - (col("Discount %") * col("Gross Sales")))
   df_orders = df_orders.withColumn("Unit Price", col("Gross Sales") / col("Order Quantity"))

   # Add Timestamps
   df_orders = df_orders.withColumn("created_at", current_timestamp()) \
                        .withColumn("updated_at", current_timestamp())

   # UPSERT to Silver (orders)
   delta_orders = DeltaTable.forPath(spark, silver_path + "orders") if spark.catalog.tableExists("delta.`" + silver_path + "orders`") else None
   if delta_orders:
       delta_orders.alias("target").merge(
           df_orders.alias("source"),
           "target.`Order ID` = source.`Order ID`"
       ).whenMatchedUpdate(
           condition="target.updated_at < source.updated_at OR target.Gross Sales != source.Gross Sales OR target.Profit != source.Profit",
           set={
               "Order Item ID": "source.Order Item ID",
               "Order YearMonth": "source.Order YearMonth",
               "Order Year": "source.Order Year",
               "Order Month": "source.Order Month",
               "Order Day": "source.Order Day",
               "Order Time": "source.Order Time",
               "Order Quantity": "source.Order Quantity",
               "Product Department": "source.Product Department",
               "Product Category": "source.Product Category",
               "Product Name": "source.Product Name",
               "Customer ID": "source.Customer ID",
               "Customer Market": "source.Customer Market",
               "Customer Region": "source.Customer Region",
               "Customer Country": "source.Customer Country",
               "Warehouse Country": "source.Warehouse Country",
               "Shipment Year": "source.Shipment Year",
               "Shipment Month": "source.Shipment Month",
               "Shipment Day": "source.Shipment Day",
               "Shipment Mode": "source.Shipment Mode",
               "Shipment Days - Scheduled": "source.Shipment Days - Scheduled",
               "Gross Sales": "source.Gross Sales",
               "Discount %": "source.Discount %",
               "Profit": "source.Profit",
               "Order Date": "source.Order Date",
               "Shipment Date": "source.Shipment Date",
               "Shipping Time": "source.Shipping Time",
               "Order DateTime": "source.Order DateTime",
               "Delay Shipment": "source.Delay Shipment",
               "Net Sales": "source.Net Sales",
               "Unit Price": "source.Unit Price",
               "updated_at": current_timestamp()
           }
       ).whenNotMatchedInsertAll().execute()
   else:
       df_orders.write.format("delta").mode("overwrite").save(silver_path + "orders")

   # Fulfillment
   fulfillment_schema = StructType([
       StructField("Product Name", StringType(), True),
       StructField("Warehouse Order Fulfillment (days)", FloatType(), True)
   ])
   df_fulfillment = spark.read.schema(fulfillment_schema).json(raw_path + "fulfillment/*.json")
   df_fulfillment = df_fulfillment.dropDuplicates().fillna({"Warehouse Order Fulfillment (days)": 0.0})
   df_fulfillment = df_fulfillment.filter(col("Warehouse Order Fulfillment (days)") >= 0)
   df_fulfillment = df_fulfillment.withColumn("created_at", current_timestamp()) \
                                  .withColumn("updated_at", current_timestamp())

   # UPSERT Fulfillment
   delta_fulfillment = DeltaTable.forPath(spark, silver_path + "fulfillment") if spark.catalog.tableExists("delta.`" + silver_path + "fulfillment`") else None
   if delta_fulfillment:
       delta_fulfillment.alias("target").merge(
           df_fulfillment.alias("source"),
           "target.`Product Name` = source.`Product Name`"
       ).whenMatchedUpdate(
           condition="target.updated_at < source.updated_at OR target.`Warehouse Order Fulfillment (days)` != source.`Warehouse Order Fulfillment (days)`",
           set={
               "Warehouse Order Fulfillment (days)": "source.Warehouse Order Fulfillment (days)",
               "updated_at": current_timestamp()
           }
       ).whenNotMatchedInsertAll().execute()
   else:
       df_fulfillment.write.format("delta").mode("overwrite").save(silver_path + "fulfillment")

   # Inventory
   inventory_schema = StructType([
       StructField("Product Name", StringType(), True),
       StructField("Year Month", IntegerType(), True),
       StructField("Warehouse Inventory", IntegerType(), True),
       StructField("Inventory Cost Per Unit", FloatType(), True)
   ])
   df_inventory = spark.read.schema(inventory_schema).option("header", "true").csv(raw_path + "inventory/*.csv")
   df_inventory = df_inventory.dropDuplicates().fillna({"Warehouse Inventory": 0, "Inventory Cost Per Unit": 0.0})
   df_inventory = df_inventory.filter(col("Warehouse Inventory") >= 0)
   matching_products = df_orders.select("Product Name").distinct().rdd.map(lambda row: row[0]).collect()
   df_inventory = df_inventory.filter(col("Product Name").isin(matching_products))
   df_inventory = df_inventory.withColumn("created_at", current_timestamp()) \
                              .withColumn("updated_at", current_timestamp())

   # UPSERT Inventory
   delta_inventory = DeltaTable.forPath(spark, silver_path + "inventory") if spark.catalog.tableExists("delta.`" + silver_path + "inventory`") else None
   if delta_inventory:
       delta_inventory.alias("target").merge(
           df_inventory.alias("source"),
           "target.`Product Name` = source.`Product Name` AND target.`Year Month` = source.`Year Month`"
       ).whenMatchedUpdate(
           condition="target.updated_at < source.updated_at OR target.`Warehouse Inventory` != source.`Warehouse Inventory` OR target.`Inventory Cost Per Unit` != source.`Inventory Cost Per Unit`",
           set={
               "Warehouse Inventory": "source.Warehouse Inventory",
               "Inventory Cost Per Unit": "source.Inventory Cost Per Unit",
               "updated_at": current_timestamp()
           }
       ).whenNotMatchedInsertAll().execute()
   else:
       df_inventory.write.format("delta").mode("overwrite").save(silver_path + "inventory")

   # Verify
   df_orders.createOrReplaceTempView("silver_orders")
   spark.sql("SELECT * FROM silver_orders LIMIT 5").show()
   ```

3. **Explanation**:
   - **Cleaning**: Removes duplicates, fills nulls (0 or 'Unknown'), filters invalid (e.g., quantity >0), handles negative shipping times, ensures inventory matches orders’ products.
   - **Timestamps**: Adds `created_at` (first insert), `updated_at` (updates on change).
   - **UPSERT**: Uses Delta Merge to update records if `updated_at` or key fields (e.g., Gross Sales, Profit) change, inserts new records. Keys: `Order ID` (orders), `Product Name` (fulfillment), `Product Name` + `Year Month` (inventory).
   - **Output**: Delta tables in `tables/silver/orders/`, etc.

4. **Test**: Run cells > Check Portal > Storage Explorer > `tables/silver/` > Verify Delta files.

Take screenshot.

---

### **Section 4: ADF Pipeline for Silver Layer (PL_LOAD_SILVER_LAYER)**
In ADF Studio:
1. **Linked Service for Databricks**:
   - Author > Linked services > New > Azure Databricks > Name `ls_databricks` > Workspace URL (from Databricks Overview) > Access token (Databricks > User settings > Generate token) > Cluster ID (Compute > `supply-chain-cluster` > Advanced options) > Create.
2. **Pipeline**:
   - Pipelines > New > Name `PL_LOAD_SILVER_LAYER`.
   - Drag Azure Databricks > Notebook > Name `run_silver_notebook` > Linked service `ls_databricks` > Notebook path: Browse > `nb_load_silver_layer` > Publish > Debug.
3. **Test**: Monitor > Pipeline runs.

Take screenshot.

---

### **Section 5: Gold Layer - Normalize and Propagate Timestamps (nb_load_gold_layer)**
Reads Silver, creates fact/dimension tables, propagates timestamps, adds KPIs.

In Databricks:
1. Create Notebook: Name `nb_load_gold_layer` > Python > Attach cluster.
2. Copy-Paste Code:
   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.functions import monotonically_increasing_id, year, month, dayofmonth, date_format, sum, count, when
   from pyspark.sql.types import IntegerType, DateType

   spark = SparkSession.builder.appName("LoadGoldLayer").getOrCreate()

   adls_key = dbutils.secrets.get(scope="supply_chain_secrets", key="adls_key")
   spark.conf.set("fs.azure.account.key.supplychainadls.dfs.core.windows.net", adls_key)

   silver_path = "abfss://tables@supplychainadls.dfs.core.windows.net/silver/"
   gold_path = "abfss://tables@supplychainadls.dfs.core.windows.net/gold/"

   # Read Silver
   df_orders = spark.read.format("delta").load(silver_path + "orders")
   df_fulfillment = spark.read.format("delta").load(silver_path + "fulfillment")
   df_inventory = spark.read.format("delta").load(silver_path + "inventory")

   # Dimension Tables
   dim_product_dept = df_orders.select(col("Product Department").alias("ProductDepartmentName")).distinct() \
       .withColumn("ProductDepartmentID", monotonically_increasing_id()) \
       .withColumn("Description", col("ProductDepartmentName")) \
       .withColumn("created_at", current_timestamp()) \
       .withColumn("updated_at", current_timestamp()) \
       .select("ProductDepartmentID", "ProductDepartmentName", "Description", "created_at", "updated_at")

   dim_product_category = df_orders.select("Product Category", "Product Department").distinct() \
       .join(dim_product_dept, col("Product Department") == col("ProductDepartmentName"), "inner") \
       .withColumn("ProductCategoryID", monotonically_increasing_id()) \
       .withColumn("Description", col("Product Category")) \
       .withColumn("created_at", current_timestamp()) \
       .withColumn("updated_at", current_timestamp()) \
       .select("ProductCategoryID", "ProductDepartmentID", col("Product Category").alias("ProductCategoryName"), "Description", "created_at", "updated_at")

   dim_product = df_orders.select("Product Name", "Product Category").distinct() \
       .join(dim_product_category, col("Product Category") == col("ProductCategoryName"), "inner") \
       .withColumn("ProductID", monotonically_increasing_id()) \
       .withColumn("Description", col("Product Name")) \
       .withColumn("created_at", current_timestamp()) \
       .withColumn("updated_at", current_timestamp()) \
       .select("ProductID", "ProductCategoryID", col("Product Name").alias("ProductName"), "Description", "created_at", "updated_at")

   dim_geography = df_orders.select("Customer Country", "Customer Region", "Customer Market").distinct() \
       .withColumn("GeographyID", monotonically_increasing_id()) \
       .withColumn("created_at", current_timestamp()) \
       .withColumn("updated_at", current_timestamp()) \
       .select("GeographyID", col("Customer Country").alias("Country"), col("Customer Region").alias("Region"), col("Customer Market").alias("Market"), "created_at", "updated_at")

   dim_customer = df_orders.select(col("Customer ID").alias("CustomerDataID"), "Customer Country").distinct() \
       .join(dim_geography, col("Customer Country") == col("Country"), "inner") \
       .withColumn("CustomerID", monotonically_increasing_id()) \
       .withColumn("CustomerName", lit("")) \
       .withColumn("created_at", current_timestamp()) \
       .withColumn("updated_at", current_timestamp()) \
       .select("CustomerID", "CustomerDataID", "CustomerName", "GeographyID", "created_at", "updated_at")

   min_date = df_orders.agg({"Order Date": "min"}).collect()[0][0]
   max_date = df_orders.agg({"Order Date": "max"}).collect()[0][0]
   dim_date = spark.range(0, (max_date - min_date).days + 1) \
       .withColumn("Date", to_date(lit(min_date)) + col("id")) \
       .withColumn("DateKey", date_format(col("Date"), "yyyyMMdd").cast(IntegerType())) \
       .withColumn("Year", year(col("Date"))) \
       .withColumn("Month", month(col("Date"))) \
       .withColumn("Day", dayofmonth(col("Date"))) \
       .withColumn("created_at", current_timestamp()) \
       .withColumn("updated_at", current_timestamp()) \
       .drop("id")

   # Fact Tables
   fact_orders = df_orders \
       .join(dim_product, col("Product Name") == col("ProductName"), "inner") \
       .join(dim_customer, col("Customer ID") == col("CustomerDataID"), "inner") \
       .join(dim_date.alias("order_date"), col("Order Date") == col("order_date.Date"), "inner") \
       .join(dim_date.alias("shipment_date"), col("Shipment Date") == col("shipment_date.Date"), "inner") \
       .withColumn("OrderID", monotonically_increasing_id()) \
       .select(
           "OrderID", col("Order ID").alias("OrderNumber"), col("Order Item ID").alias("OrderItemID"),
           col("order_date.DateKey").alias("OrderDateKey"), col("Order DateTime").alias("OrderDate"),
           "Order Quantity", "ProductID", "CustomerID", "Gross Sales", "Discount %", "Profit",
           "Net Sales", "Unit Price", col("Delay Shipment").alias("Shipment Feature"),
           col("orders.created_at"), col("orders.updated_at")
       )

   fact_shipment = df_orders \
       .join(fact_orders, col("Order ID") == col("OrderNumber"), "inner") \
       .join(dim_date.alias("shipment_date"), col("Shipment Date") == col("shipment_date.Date"), "inner") \
       .withColumn("ShipmentID", monotonically_increasing_id()) \
       .select(
           "ShipmentID", "OrderID", col("shipment_date.DateKey").alias("ShipmentDateKey"), "Shipment Mode",
           col("orders.created_at"), col("orders.updated_at")
       )

   dim_fulfillment = df_fulfillment \
       .join(dim_product, col("Product Name") == col("ProductName"), "inner") \
       .withColumn("FulfillmentID", monotonically_increasing_id()) \
       .select(
           "FulfillmentID", "ProductID", col("Product Name"), col("Warehouse Order Fulfillment (days)"),
           col("fulfillment.created_at"), col("fulfillment.updated_at")
       )

   fact_inventory = df_inventory \
       .withColumn("Storage Cost", col("Inventory Cost Per Unit") * col("Warehouse Inventory")) \
       .join(dim_product, col("Product Name") == col("ProductName"), "inner") \
       .withColumn("InventoryID", monotonically_increasing_id()) \
       .select(
           "InventoryID", "ProductID", "Year Month", "Warehouse Inventory", "Inventory Cost Per Unit", "Storage Cost",
           col("inventory.created_at"), col("inventory.updated_at")
       )

   fact_monthly_orders = df_orders.groupBy("Order YearMonth") \
       .agg(
           sum("Net Sales").alias("Total Net Sales"),
           sum("Profit").alias("Total Profit"),
           count("Order ID").alias("Total Orders"),
           sum(when(col("Delay Shipment") == "Late", 1).otherwise(0)).alias("Late Orders")
       ) \
       .withColumn("Profit Margin", col("Total Profit") / col("Total Net Sales")) \
       .withColumn("Late Shipment Rate", col("Late Orders") / col("Total Orders")) \
       .withColumn("MonthlyOrdersID", monotonically_increasing_id()) \
       .withColumn("created_at", current_timestamp()) \
       .withColumn("updated_at", current_timestamp())

   # Write to Gold
   dim_product_dept.write.format("delta").mode("overwrite").save(gold_path + "DimProductDepartment")
   dim_product_category.write.format("delta").mode("overwrite").save(gold_path + "DimProductCategory")
   dim_product.write.format("delta").mode("overwrite").save(gold_path + "DimProduct")
   dim_geography.write.format("delta").mode("overwrite").save(gold_path + "DimGeography")
   dim_customer.write.format("delta").mode("overwrite").save(gold_path + "DimCustomer")
   dim_date.write.format("delta").mode("overwrite").save(gold_path + "DimDate")
   fact_orders.write.format("delta").mode("overwrite").save(gold_path + "FactOrders")
   fact_shipment.write.format("delta").mode("overwrite").save(gold_path + "FactShipment")
   dim_fulfillment.write.format("delta").mode("overwrite").save(gold_path + "DimFulfillment")
   fact_inventory.write.format("delta").mode("overwrite").save(gold_path + "FactInventory")
   fact_monthly_orders.write.format("delta").mode("overwrite").save(gold_path + "FactMonthlyOrders")
   ```

3. **Test**: Run > Check `tables/gold/` in Storage Explorer.

Take screenshot.

---

### **Section 6: ADF Pipeline for Gold Layer (PL_LOAD_GOLD_LAYER)**
In ADF Studio:
1. Pipeline: Pipelines > New > Name `PL_LOAD_GOLD_LAYER` > Drag Notebook > Name `run_gold_notebook` > Linked `ls_databricks` > Notebook `nb_load_gold_layer` > Publish > Debug.
2. Test: Monitor > Pipeline runs.

Take screenshot.

---

### **Section 7: Register Tables in Unity Catalog**
In Databricks:
1. Create Notebook: Name `nb_register_tables` > SQL > Attach cluster.
2. Copy-Paste:
   ```sql
   CREATE TABLE IF NOT EXISTS supply_chain_catalog.supply_chain_schema.DimProductDepartment
   USING DELTA
   LOCATION 'abfss://tables@supplychainadls.dfs.core.windows.net/gold/DimProductDepartment';

   -- Repeat for DimProductCategory, DimProduct, DimGeography, DimCustomer, DimDate, FactOrders, FactShipment, DimFulfillment, FactInventory, FactMonthlyOrders
   ```
3. Test: Catalog > supply_chain_schema > Verify tables > Run `SELECT * FROM supply_chain_catalog.supply_chain_schema.FactOrders LIMIT 5`.

Take screenshot.

---

### **Section 8: Master Pipeline and Scheduling (PL_LOAD_SUPPLYCHAIN_DATA_MASTER)**
In ADF Studio:
1. Pipeline: Pipelines > New > Name `PL_LOAD_SUPPLYCHAIN_DATA_MASTER`.
   - Drag Execute Pipeline > Name `exec_bronze` > Pipeline `PL_COPY_RAW_FILES`.
   - Drag Execute Pipeline > Name `exec_silver` > Pipeline `PL_LOAD_SILVER_LAYER` > Connect from `exec_bronze` (green arrow).
   - Drag Execute Pipeline > Name `exec_gold` > Pipeline `PL_LOAD_GOLD_LAYER` > Connect from `exec_silver`.
   - Add Web activities after each:
     - Name: `notify_success_<pipeline>` > URL: Logic App URL > Method: POST > Body: `{"message": "Pipeline @{pipeline().Pipeline} succeeded", "email": "your@email.com"}`.
     - On failure (red arrow): Web `notify_failure_<pipeline>` > Body: `{"message": "Pipeline @{pipeline().Pipeline} failed: @{activity('prev').error.message}", "email": "your@email.com"}`.
2. Trigger: Triggers > New > Name `trg_daily_load` > Schedule > Every 1 day > Start today > Time 12:05 UTC > Activated: Yes > Associate with master pipeline.
3. Test: Publish > Trigger now > Check emails.

Take screenshot.

---

### **Section 9: Power BI Dashboards**
In Power BI Desktop:
1. Connect: Get Data > Azure Databricks > Server: Databricks URL > HTTP Path: From cluster > Database: `supply_chain_catalog.supply_chain_schema` > Load all tables.
2. Dashboards (2+ questions per section):
   - Business: Cards (Total Net Sales, Profit Margin), Line (Sales over YearMonth), Bar (Sales by Department).
   - Customer: Map (Country distribution), Line (Customers over time).
   - Product: Bar (Top categories by orders), Bar (Profit by product).
   - Inventory: Bar (Storage cost by department), Line (Inventory over Year Month).
   - Shipment: Pie (Shipment modes), Bar (Late rate by department).
   - Add slicers (Year, Department).
3. Publish: File > Publish > Power BI Service.

Take screenshots.

---

### **Section 10: Submission**
1. PPT: Create in PowerPoint > Add screenshots (resources, pipelines, notebooks, dashboards) with name/ID visible.
2. Export Notebooks: Databricks > Notebooks > Export > DBC Archive.
3. ZIP: Local folder `capstone_submission` > Add .dbc, PPT, screenshots > ZIP (<100MB).
4. Submit: Upload to course platform.

---

### **Business Improvements**
- **Late Shipments**: High rates (from FactMonthlyOrders) suggest logistics issues. Propose new warehouses or faster modes.
- **Inventory**: Overstocking (FactInventory) indicates poor forecasting. Use historical sales for predictions.
- **Sales**: Declines in departments (FactOrders) may need supplier fixes.

---

### **Testing**
- Debug each pipeline/notebook.
- Check ADLS containers (`raw`, `tables/silver`, `tables/gold`).
- Verify UPSERT: Upload modified data to `source`, run pipeline, check `updated_at` changes.

<xaiArtifact artifact_id="de7a62bc-dcff-4e59-b965-557f87e76a57" artifact_version_id="a5e9fef0-d294-463f-98ec-4488b08348e2" title="supply_chain_implementation.md" contentType="text/markdown">
# E-Commerce Supply Chain Capstone Implementation

## Section 1: Azure Resources
- **Resource Group**: `supply-chain-rg` in East US.
- **ADLS**: `supplychainadls` with containers `source`, `raw`, `tables` (subfolders `bronze`, `silver`, `gold`).
- **Databricks**: `supply-chain-databricks`, cluster `supply-chain-cluster`, catalog `supply_chain_catalog`, schema `supply_chain_schema`.
- **Secrets**: Scope `supply_chain_secrets`, key `adls_key`.
- **ADF**: `supply-chain-adf`.
- **Logic App**: `supply-chain-notifications` with HTTP trigger.

## Section 2: Bronze Pipeline (PL_COPY_RAW_FILES)
- Checks files in `source` (2 CSVs in `ordersandshipment/`, 1 JSON in `fulfillment/`, 1 CSV in `inventory/`).
- Copies to `raw`.
- Deletes from `source` on success.
- Flat pipeline with Get Metadata, If Condition, Copy, Delete, Web activities.

## Section 3: Silver Notebook (nb_load_silver_layer)
- Reads from `raw`.
- Cleans: Drops duplicates, fills nulls, filters invalid, fixes shipping times, matches products.
- Adds timestamps: `created_at`, `updated_at`.
- UPSERTs using Delta Merge.
- Writes to `tables/silver/`.

## Section 4: Gold Notebook (nb_load_gold_layer)
- Creates fact/dimension tables with KPIs (Profit Margin, Late Shipment Rate, Storage Cost).
- Propagates timestamps.
- Writes to `tables/gold/`.

## Section 5: Pipelines and Scheduling
- Flat pipelines: `PL_LOAD_SILVER_LAYER`, `PL_LOAD_GOLD_LAYER`, `PL_LOAD_SUPPLYCHAIN_DATA_MASTER`.
- Trigger: `trg_daily_load` at 12:05 UTC.

## Section 6: Power BI
- Connects to Databricks tables.
- Dashboards for Sales, Customer, Product, Inventory, Shipment.

## Section 7: Submission
- ZIP with .dbc, PPT, screenshots.
</xaiArtifact>
