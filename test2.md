### **Section 1: GitHub Repo Setup (Source for Raw Data Copying)**
In GitHub web UI (github.com):
1. Create repo: "+" > New repository > Name `supply-chain-capstone` > Create.
2. Upload data: "Add file" > Upload > Create folders and add files (e.g., data/ordersandshipment/orders_2015.csv, data/fulfillment/fulfillment.json, data/inventory/inventory.csv).
3. Add docs folder for PPT/screenshots.

---

### **Section 2: Azure Resources Setup in Portal UI**
In Azure Portal (portal.azure.com):
1. Resource Group: Search "Resource groups" > Create > Name `supply-chain-rg` > Create.
2. ADLS: Search "Storage accounts" > Create > Name `supplychainadls` > Enable hierarchical namespace > Create. Then, in storage > Containers > New: `bronze`, `silver`, `gold`.
3. Copy ADLS Key: Storage account > Access keys > Copy key1.
4. Databricks: Search "Azure Databricks" > Create > Name `supply-chain-databricks` > Premium > Create > Launch Workspace.
   - In Databricks UI (launched workspace): Compute > Create compute > Name `supply-chain-cluster` > Single node > Create.
   - Catalog > Create catalog > Name `supply_chain_catalog` > Storage: Select ADLS gold container.
   - Create schema inside catalog: `supply_chain_schema`.
5. Secrets: In Databricks > New Notebook > Run code to create scope and put ADLS key (as in previous).
6. ADF: Search "Data factories" > Create > Name `supply-chain-adf` > Enable GitHub integration with your repo > Create > Launch Studio.
7. Logic App: Search "Logic Apps" > Create > Name `supply-chain-notifications` > Create. In Designer: Add HTTP request trigger > Add Send email action > Save > Copy URL.

---

### **Section 3: Bronze Layer - Copy Raw Data from GitHub to ADLS (PL_COPY_RAW_FILES)**
This copies raw data (no cleaning yet) to Bronze.

In ADF Studio (adf.azure.com > Your factory):
1. Linked Services: Author > Linked services > New > HTTP > Name `ls_github_http` > Base URL `https://raw.githubusercontent.com/yourusername/supply-chain-capstone/main/` > Anonymous > Create.
2. New > ADLS Gen2 > Name `ls_adls_gen2` > Account key auth > Paste key > Account `supplychainadls` > Test > Create.
3. Datasets: Author > Datasets > New > HTTP > DelimitedText > Name `ds_github_orders` > Linked `ls_github_http` > Path `data/ordersandshipment/*.csv` > Header yes > Create.
   - Similar for `ds_github_fulfillment` (JSON format), `ds_github_inventory` (DelimitedText).
   - Sink: New > ADLS Gen2 > DelimitedText > Name `ds_bronze_orders` > Linked `ls_adls_gen2` > Folder `bronze/ordersandshipment` > Create. Repeat for others.
4. Pipeline: Author > Pipelines > New > Name `PL_COPY_RAW_FILES`.
   - Drag Copy data activity > Name `copy_orders` > Source tab: Dataset `ds_github_orders` > Wildcard `*.csv`.
   - Sink tab: `ds_bronze_orders` > File name from source.
   - Add Copy activities for fulfillment/inventory.
5. Publish > Debug > Check ADLS in Portal (Storage > Containers > bronze) for copied files.

This is where **copying data** happens—raw CSVs/JSON from GitHub to ADLS Bronze using ADF UI.

---

### **Section 4: Silver Layer - Clean and Transform Data (nb_load_silver_layer)**
This is where **cleaning data** occurs: Define schema, remove duplicates/nulls/invalid, add features (e.g., dates, shipment delays, net sales). Data is read from Bronze, cleaned, written to Silver as Delta.

In Databricks Workspace UI:
1. Notebooks > Create Notebook > Name `nb_load_silver_layer` > Python > Attach cluster.
2. Copy-paste the code below into cells (run one by one for testing). This handles **how to add/clean data**:
   - Schema: Enforces datatypes.
   - Cleaning: Drop dups, fill nulls (0 for numerics, 'Unknown' for strings), filter invalid (e.g., quantity >0, sales >=0), remove special chars, drop bad shipping times.
   - Features: Add Order DateTime, Shipping Time, Delay Shipment (Late/On Time), Net Sales, Unit Price.
   - For inconsistencies: Filter inventory to match orders' products.

Code:
```
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, concat, lit, when, datediff, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

spark = SparkSession.builder.appName("LoadSilverLayer").getOrCreate()

adls_key = dbutils.secrets.get(scope="supply_chain_secrets", key="adls_key")
spark.conf.set("fs.azure.account.key.supplychainadls.dfs.core.windows.net", adls_key)

bronze_path = "abfss://bronze@supplychainadls.dfs.core.windows.net/"
silver_path = "abfss://silver@supplychainadls.dfs.core.windows.net/"

# Add/Define Schema for Orders (prevents type issues)
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

# Read from Bronze (copying/using data)
df_orders = spark.read.schema(orders_schema).option("header", "true").csv(bronze_path + "ordersandshipment/*.csv")
df_orders.cache()  # For faster cleaning
df_orders.printSchema()  # Verify datatypes
df_orders.show(5)  # Check first records
print(f"Total rows before clean: {df_orders.count()}")

# Clean Data: Remove duplicates
df_orders = df_orders.dropDuplicates()

# Clean: Handle nulls/missing (fill appropriate values)
df_orders = df_orders.fillna(0, subset=["Order Quantity", "Gross Sales", "Discount %", "Profit", "Shipment Days - Scheduled"])
df_orders = df_orders.fillna("Unknown", subset=["Product Department", "Product Category", "Product Name", "Customer Market", "Customer Region", "Customer Country", "Shipment Mode"])

# Clean: Remove invalid data (business rules: positive qty/sales, etc.)
df_orders = df_orders.filter((col("Order Quantity") > 0) & (col("Gross Sales") >= 0) & (col("Discount %") >= 0) & (col("Discount %") <= 1))

# Verify after clean: Stats, max/min, duplicates check
df_orders.describe().show()
df_orders.agg({"Gross Sales": "max", "Gross Sales": "min"}).show()
duplicates = df_orders.groupBy("Order ID").count().filter("count > 1")
duplicates.show()  # Should be empty if clean

# Special Clean: Remove special chars in Country, add dates for features
df_orders = df_orders.withColumn("Customer Country", regexp_replace(col("Customer Country"), "[^a-zA-Z0-9 ]", ""))
df_orders = df_orders.withColumn("Order Date", to_date(concat(col("Order Year"), lit("-"), col("Order Month"), lit("-"), col("Order Day"))))
df_orders = df_orders.withColumn("Shipment Date", to_date(concat(col("Shipment Year"), lit("-"), col("Shipment Month"), lit("-"), col("Shipment Day"))))

# Clean: Handle negative/unusual shipping times (drop <0 or >28)
df_orders = df_orders.withColumn("Shipping Time", datediff(col("Shipment Date"), col("Order Date")))
df_orders = df_orders.filter((col("Shipping Time") >= 0) & (col("Shipping Time") <= 28))

# Add Features (as per requirements)
df_orders = df_orders.withColumn("Order DateTime", concat(col("Order Date"), lit(" "), col("Order Time")))
df_orders = df_orders.withColumn("Delay Shipment", when(col("Shipping Time") > col("Shipment Days - Scheduled"), "Late").otherwise("On Time"))
df_orders = df_orders.withColumn("Net Sales", col("Gross Sales") - (col("Discount %") * col("Gross Sales")))
df_orders = df_orders.withColumn("Unit Price", col("Gross Sales") / col("Order Quantity"))

print(f"Total rows after clean: {df_orders.count()}")

# Fulfillment: Similar schema/clean
fulfillment_schema = StructType([StructField("Product Name", StringType(), True), StructField("Warehouse Order Fulfillment (days)", FloatType(), True)])
df_fulfillment = spark.read.schema(fulfillment_schema).json(bronze_path + "fulfillment/*.json")
df_fulfillment = df_fulfillment.dropDuplicates().fillna({"Warehouse Order Fulfillment (days)": 0.0})
df_fulfillment.filter(col("Warehouse Order Fulfillment (days)") > 0)  # Validate

# Inventory: Schema/clean, handle inconsistencies (match products to orders)
inventory_schema = StructType([StructField("Product Name", StringType(), True), StructField("Year Month", IntegerType(), True), StructField("Warehouse Inventory", IntegerType(), True), StructField("Inventory Cost Per Unit", FloatType(), True)])
df_inventory = spark.read.schema(inventory_schema).option("header", "true").csv(bronze_path + "inventory/*.csv")
df_inventory = df_inventory.dropDuplicates().fillna(0).filter(col("Warehouse Inventory") >= 0)
matching_products = df_orders.select("Product Name").distinct().rdd.map(lambda row: row[0]).collect()
df_inventory = df_inventory.filter(col("Product Name").isin(matching_products))  # Clean unmatched

# Write cleaned to Silver (using Delta for ACID)
df_orders.write.format("delta").mode("overwrite").save(silver_path + "orders")
df_fulfillment.write.format("delta").mode("overwrite").save(silver_path + "fulfillment")
df_inventory.write.format("delta").mode("overwrite").save(silver_path + "inventory")

# Add SQL View for queries/EDA
df_orders.createOrReplaceTempView("silver_orders")
spark.sql("SELECT AVG(Net Sales) AS avg_sales FROM silver_orders").show()  # Example insight
```

3. Run cells. This **uses data** from Bronze, cleans/adds, writes to Silver. Check in Portal > Storage > silver for Delta folders.

---

### **Section 5: ADF Pipeline PL_LOAD_SILVER_LAYER**
In ADF Studio: Pipelines > New > Name `PL_LOAD_SILVER_LAYER` > Drag Notebook activity > Linked service (create Databricks LS if not) > Notebook `nb_load_silver_layer` > Publish > Debug.

---

### **Section 6: Gold Layer - Normalize and Add KPIs (nb_load_gold_layer)**
Read from Silver, add surrogates/fact/dim, KPIs (e.g., profit margin, storage cost, late rate).

In Databricks: Create `nb_load_gold_layer`, paste code (run).

Code (emphasizing KPIs):
```
from pyspark.sql.functions import monotonically_increasing_id, year, month, dayofmonth, date_format, sum, count, when
gold_path = "abfss://gold@supplychainadls.dfs.core.windows.net/"

# Use/Copy from Silver
df_orders = spark.read.format("delta").load(silver_path + "orders")
df_fulfillment = spark.read.format("delta").load(silver_path + "fulfillment")
df_inventory = spark.read.format("delta").load(silver_path + "inventory")

# Add Dims with Surrogates
dim_product_dept = df_orders.select(col("Product Department").alias("ProductDepartmentName")).distinct().withColumn("ProductDepartmentID", monotonically_increasing_id()).withColumn("Description", col("ProductDepartmentName"))
# Similar for DimProductCategory, DimProduct, DimGeography, DimCustomer, DimDate (generate from min/max dates)

# FactOrders (with KPIs from Silver)
fact_orders = df_orders [joins dims] .withColumn("OrderID", monotonically_increasing_id()) .select(...)  # As in previous code

# FactShipment: Similar join

# DimFulfillment: Join to product dim

# FactInventory: Add Storage Cost KPI
fact_inventory = df_inventory.withColumn("Storage Cost", col("Inventory Cost Per Unit") * col("Warehouse Inventory")) [join dim, add ID]

# FactMonthlyOrders: Aggregate KPIs
fact_monthly_orders = df_orders.groupBy("Order YearMonth").agg(sum("Net Sales").alias("Total Net Sales"), ...).withColumn("Profit Margin", col("Total Profit") / col("Total Net Sales")).withColumn("Late Shipment Rate", col("Late Orders") / col("Total Orders"))

# Write to Gold
[write each as delta]
```
