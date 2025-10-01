### Explanation of DIM Tables (Recap and Expansion)

DIM (Dimension) tables are key components in a star schema for data warehouses, providing descriptive attributes for analysis. They help in filtering and grouping data in Power BI. As explained before, configure them by:

- Identifying unique entities (e.g., products, dates) from the dataset.
- Adding surrogate keys (unique IDs) for relationships.
- Denormalizing for performance (include hierarchies like department > category).
- Handling changes (e.g., SCD Type 1: overwrite).
- Writing as Delta tables in Silver for use in Power BI (import, set keys as relationships).

For these individual datasets, I'll create relevant DIMs per notebook:
- Shared DIMs (e.g., DIM_PRODUCT) can be created in multiple if needed, but in practice, merge in Gold layer.
- Maximize DIMs: e.g., DIM_DATE from dates, DIM_LOCATION from countries/regions, DIM_SHIPMENT_MODE from modes, DIM_ABCXYZ from segments, etc.

Cleaning adapted from the notebook: load, drop NA/duplicates, date conversions, feature engineering (e.g., Net Sales, Storage Cost, segments), insights via groupbys.

Assumptions:
- Bronze paths: /bronze/inventory_delta, /bronze/fulfillment_delta, /bronze/orders_shipments_delta.
- Data at lowest granularity (e.g., per product-month for inventory).
- Schemas inferred from summary.

### 1. Notebook for Inventory Dataset (PL_LOAD_SILVER_INVENTORY)

This handles inventory data: cleaning, quality checks, DIMs like DIM_PRODUCT, DIM_ABCXYZ, DIM_DATE (from Year Month), FACT_INVENTORY.

```python
# Databricks notebook source
# # PL_LOAD_SILVER_INVENTORY Notebook
# Reads from Bronze inventory Delta, performs cleaning/transformations (duplicates removal, filters, cost calculations), data quality, and loads to Silver.
# Creates DIMs for Power BI.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SilverInventoryLoad").getOrCreate()

# Step 1: Define schema
inventory_schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("Product_Name", StringType(), True),
    StructField("Year_Month", StringType(), True),  # e.g., '2015-01'
    StructField("Warehouse_Inventory", IntegerType(), True),
    StructField("Inventory_Cost_Per_Unit", DoubleType(), True),
    StructField("Product_Category", StringType(), True),
    StructField("Product_Department", StringType(), True),
    StructField("Storage_Cost", DoubleType(), True),  # Calculated
    StructField("ABCXYZ", StringType(), True)  # Segment
])

# Step 2: Read from Bronze Delta
df_inventory = spark.read.format("delta").load("/bronze/inventory_delta")

# COMMAND ----------

# ## Data Verification and Quality Checks

# COMMAND ----------

# Step 3: Verify schema
print("Inventory Schema:")
df_inventory.printSchema()

# Step 4: Check datatypes
print("Inventory dtypes:", df_inventory.dtypes)

# Step 5: Cache DataFrame
df_inventory.cache()

# Step 6: Verify first few records
print("Inventory head:")
df_inventory.show(5, truncate=False)

# Step 7: Clean data - remove duplicates, nulls, invalid
df_inventory = df_inventory.dropna().dropDuplicates()
df_inventory = df_inventory.filter(col("Warehouse_Inventory") > 10)  # As per notebook

# Date transformation
df_inventory = df_inventory.withColumn("Year_Month_Date", to_date(concat(col("Year_Month"), lit("-01")), "yyyy-MM-dd"))

# Calculate Storage Cost if not present
df_inventory = df_inventory.withColumn("Storage_Cost", col("Warehouse_Inventory") * col("Inventory_Cost_Per_Unit"))

# Step 8: Data accuracy (business rules, e.g., costs positive)
df_inventory = df_inventory.filter((col("Inventory_Cost_Per_Unit") > 0) & (col("Storage_Cost") > 0))

# Step 9: Completeness - check missing
print("Inventory missing per col:", [df_inventory.filter(col(c).isNull()).count() for c in df_inventory.columns])

# Step 10: Consistency - standardize names
df_inventory = df_inventory.withColumn("Product_Name", trim(lower(col("Product_Name"))))

# Step 11: Rows/columns
print("Inventory rows/cols:", df_inventory.count(), len(df_inventory.columns))

# Step 12: Summary stats
print("Inventory describe:")
df_inventory.describe().show()

# Step 13: Max/min
numeric_cols = [f.name for f in df_inventory.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType))]
df_inventory.select([max(col(c)).alias(f"max_{c}") for c in numeric_cols] + [min(col(c)).alias(f"min_{c}") for c in numeric_cols]).show()

# Step 14: Duplicates in columns (e.g., Product_Name + Year_Month unique)
df_inventory.groupBy("Product_Name", "Year_Month").count().filter("count > 1").show()

# COMMAND ----------

# ## Create Table/View

# COMMAND ----------

df_inventory.createOrReplaceTempView("inventory_view")
spark.sql("SELECT COUNT(*) FROM inventory_view").show()
spark.sql("SELECT * FROM inventory_view LIMIT 5").show()

# COMMAND ----------

# ## Dimensional Modeling - DIM Tables

# COMMAND ----------

# DIM_PRODUCT (from inventory)
dim_product = df_inventory.select("Product_Name", "Product_Category", "Product_Department").distinct() \
    .withColumn("ProductKey", monotonically_increasing_id())
dim_product.write.format("delta").mode("overwrite").save("/silver/dim_product")

# DIM_ABCXYZ (segments)
dim_abcxyz = df_inventory.select("ABCXYZ").distinct() \
    .withColumn("ABCXYZ_Key", monotonically_increasing_id()) \
    .withColumn("Description", when(col("ABCXYZ") == "AX", "High value, stable").otherwise("Other"))  # Add desc
dim_abcxyz.write.format("delta").mode("overwrite").save("/silver/dim_abcxyz")

# DIM_DATE (from Year_Month)
dim_date = df_inventory.select("Year_Month_Date").distinct() \
    .withColumn("DateKey", date_format(col("Year_Month_Date"), "yyyyMMdd").cast(IntegerType())) \
    .withColumn("Year", year(col("Year_Month_Date"))) \
    .withColumn("Month", month(col("Year_Month_Date")))
dim_date.write.format("delta").mode("overwrite").save("/silver/dim_date_inventory")

# COMMAND ----------

# ## FACT_INVENTORY (lowest granularity: per product-month)

# COMMAND ----------

fact_inventory = df_inventory \
    .join(dim_product, df_inventory.Product_Name == dim_product.Product_Name, "left") \
    .join(dim_date, df_inventory.Year_Month_Date == dim_date.Year_Month_Date, "left") \
    .join(dim_abcxyz, df_inventory.ABCXYZ == dim_abcxyz.ABCXYZ, "left") \
    .select(
        col("dim_product.ProductKey"),
        col("dim_date.DateKey"),
        col("dim_abcxyz.ABCXYZ_Key"),
        col("Warehouse_Inventory"),
        col("Inventory_Cost_Per_Unit"),
        col("Storage_Cost")
    )
fact_inventory.write.format("delta").mode("overwrite").save("/silver/fact_inventory")

# COMMAND ----------

# ## Step 16: Analysis and Insights

# COMMAND ----------

# Example analysis: Inventory by segment
spark.sql("""
    SELECT ABCXYZ, AVG(Warehouse_Inventory) as Avg_Inventory, SUM(Storage_Cost) as Total_Storage
    FROM inventory_view
    GROUP BY ABCXYZ
""").show()

# Insights: 118 unique products, 5 with no demand; high storage costs for high-inventory items; AX segments stable but costly; potential overstock in certain departments.
df_inventory.unpersist()

```

### 2. Notebook for Fulfillment Dataset (PL_LOAD_SILVER_FULFILLMENT)

This handles fulfillment: simple dataset, DIM_PRODUCT (with fulfillment attrs), FACT_FULFILLMENT.

```python
# Databricks notebook source
# # PL_LOAD_SILVER_FULFILLMENT Notebook
# Reads from Bronze fulfillment Delta, cleaning (sort, merges if needed), quality, loads to Silver.
# Creates DIMs.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SilverFulfillmentLoad").getOrCreate()

# Step 1: Define schema
fulfillment_schema = StructType([
    StructField("Product_Name", StringType(), True),
    StructField("Warehouse_Order_Fulfillment_days", DoubleType(), True)
])

# Step 2: Read from Bronze
df_fulfillment = spark.read.format("delta").load("/bronze/fulfillment_delta")

# COMMAND ----------

# ## Data Verification and Quality Checks

# COMMAND ----------

# Steps 3-6
print("Fulfillment Schema:")
df_fulfillment.printSchema()
print("Fulfillment dtypes:", df_fulfillment.dtypes)
df_fulfillment.cache()
print("Fulfillment head:")
df_fulfillment.show(5, truncate=False)

# Step 7: Clean
df_fulfillment = df_fulfillment.dropna().dropDuplicates()

# Step 8: Accuracy (days > 0)
df_fulfillment = df_fulfillment.filter(col("Warehouse_Order_Fulfillment_days") > 0)

# Step 9: Missing
print("Fulfillment missing:", [df_fulfillment.filter(col(c).isNull()).count() for c in df_fulfillment.columns])

# Step 10: Consistency
df_fulfillment = df_fulfillment.withColumn("Product_Name", trim(lower(col("Product_Name"))))

# Step 11: Rows/cols
print("Fulfillment rows/cols:", df_fulfillment.count(), len(df_fulfillment.columns))

# Step 12: Stats
df_fulfillment.describe().show()

# Step 13: Max/min
df_fulfillment.select(max("Warehouse_Order_Fulfillment_days").alias("max_days"), min("Warehouse_Order_Fulfillment_days").alias("min_days")).show()

# Step 14: Duplicates
df_fulfillment.groupBy("Product_Name").count().filter("count > 1").show()

# COMMAND ----------

# ## Create View

# COMMAND ----------

df_fulfillment.createOrReplaceTempView("fulfillment_view")
spark.sql("SELECT * FROM fulfillment_view LIMIT 5").show()

# COMMAND ----------

# ## DIM Tables

# COMMAND ----------

# DIM_PRODUCT (fulfillment-focused)
dim_product = df_fulfillment.select("Product_Name").distinct() \
    .withColumn("ProductKey", monotonically_increasing_id()) \
    .withColumn("Avg_Fulfillment_Days", lit(0))  # Placeholder, can merge later
dim_product.write.format("delta").mode("overwrite").save("/silver/dim_product_fulfillment")

# DIM_FULFILLMENT_SEGMENT (if ABCXYZ added via merge, but since not, simple)
# Assume merge with inventory for ABCXYZ if needed; here basic

# COMMAND ----------

# ## FACT_FULFILLMENT (per product)

# COMMAND ----------

fact_fulfillment = df_fulfillment \
    .join(dim_product, df_fulfillment.Product_Name == dim_product.Product_Name, "left") \
    .select(
        col("dim_product.ProductKey"),
        col("Warehouse_Order_Fulfillment_days")
    )
fact_fulfillment.write.format("delta").mode("overwrite").save("/silver/fact_fulfillment")

# COMMAND ----------

# ## Analysis and Insights

# COMMAND ----------

# Top fulfillment times
spark.sql("""
    SELECT Product_Name, Warehouse_Order_Fulfillment_days
    FROM fulfillment_view
    ORDER BY Warehouse_Order_Fulfillment_days DESC
    LIMIT 10
""").show()

# Insights: Avg 5.33 days; high variance by product; impacts supply chain efficiency; integrate with inventory for full view.
df_fulfillment.unpersist()

```

### 3. Notebook for Orders and Shipments Dataset (PL_LOAD_SILVER_ORDERS_SHIPMENTS)

This is the largest: DIM_DATE, DIM_CUSTOMER, DIM_LOCATION, DIM_SHIPMENT_MODE, DIM_PRODUCT, FACT_ORDERS_SHIPMENTS.

```python
# Databricks notebook source
# # PL_LOAD_SILVER_ORDERS_SHIPMENTS Notebook
# Reads from Bronze orders_shipments Delta, cleaning (dates, net sales, delays), quality, loads to Silver.
# Creates multiple DIMs.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SilverOrdersShipmentsLoad").getOrCreate()

# Step 1: Define schema (partial, based on summary)
orders_shipments_schema = StructType([
    StructField("Order_ID", IntegerType(), True),
    StructField("Order_YearMonth", StringType(), True),
    StructField("Order_Year", IntegerType(), True),
    StructField("Order_Month", IntegerType(), True),
    StructField("Order_Day", IntegerType(), True),
    StructField("Order_Quantity", IntegerType(), True),
    StructField("Product_Department", StringType(), True),
    StructField("Product_Category", StringType(), True),
    StructField("Product_Name", StringType(), True),
    StructField("Customer_ID", IntegerType(), True),
    StructField("Customer_Market", StringType(), True),
    StructField("Customer_Region", StringType(), True),
    StructField("Customer_Country", StringType(), True),
    StructField("Warehouse_Country", StringType(), True),
    StructField("Shipment_Year", IntegerType(), True),
    StructField("Shipment_Month", IntegerType(), True),
    StructField("Shipment_Day", IntegerType(), True),
    StructField("Shipment_Mode", StringType(), True),
    StructField("Shipment_Days_Scheduled", IntegerType(), True),
    StructField("Gross_Sales", DoubleType(), True),
    StructField("Discount_Percent", DoubleType(), True),
    StructField("Profit", DoubleType(), True),
    StructField("Order_Date", StringType(), True),
    StructField("Shipment_Date", StringType(), True),
    StructField("Shipment_YearMonth", StringType(), True),
    StructField("Shipping_Time", IntegerType(), True),
    StructField("Delay_Shipment", IntegerType(), True),
    StructField("Net_Sales", DoubleType(), True),
    StructField("Unit_Price", DoubleType(), True)
])

# Step 2: Read from Bronze
df_orders_shipments = spark.read.format("delta").load("/bronze/orders_shipments_delta")

# COMMAND ----------

# ## Data Verification and Quality Checks

# COMMAND ----------

# Steps 3-6
print("Orders Shipments Schema:")
df_orders_shipments.printSchema()
print("dtypes:", df_orders_shipments.dtypes)
df_orders_shipments.cache()
df_orders_shipments.show(5, truncate=False)

# Step 7: Clean
df_orders_shipments = df_orders_shipments.dropna().dropDuplicates()

# Date conversions
df_orders_shipments = df_orders_shipments.withColumn("Order_Date", to_date(col("Order_Date"), "yyyy-MM-dd"))
df_orders_shipments = df_orders_shipments.withColumn("Shipment_Date", to_date(col("Shipment_Date"), "yyyy-MM-dd"))

# Calculate Net_Sales if not present
df_orders_shipments = df_orders_shipments.withColumn("Net_Sales", col("Gross_Sales") * (1 - col("Discount_Percent")))

# Step 8: Accuracy (quantity > 0, etc.)
df_orders_shipments = df_orders_shipments.filter((col("Order_Quantity") > 0) & (col("Net_Sales") > 0) & (col("Discount_Percent") >= 0) & (col("Discount_Percent") <= 1))

# Step 9: Missing
print("Missing per col:", [df_orders_shipments.filter(col(c).isNull()).count() for c in df_orders_shipments.columns])

# Step 10: Consistency (e.g., standardize countries)
df_orders_shipments = df_orders_shipments.withColumn("Customer_Country", upper(col("Customer_Country")))

# Step 11: Rows/cols
print("Rows/cols:", df_orders_shipments.count(), len(df_orders_shipments.columns))

# Step 12: Stats
df_orders_shipments.describe().show()

# Step 13: Max/min numeric
numeric_cols = [f.name for f in df_orders_shipments.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType))]
df_orders_shipments.select([max(col(c)).alias(f"max_{c}") for c in numeric_cols] + [min(col(c)).alias(f"min_{c}") for c in numeric_cols]).show()

# Step 14: Duplicates (Order_ID unique)
df_orders_shipments.groupBy("Order_ID").count().filter("count > 1").show()

# COMMAND ----------

# ## Create View

# COMMAND ----------

df_orders_shipments.createOrReplaceTempView("orders_shipments_view")
spark.sql("SELECT * FROM orders_shipments_view LIMIT 5").show()

# COMMAND ----------

# ## DIM Tables (Maximized)

# COMMAND ----------

# DIM_PRODUCT
dim_product = df_orders_shipments.select("Product_Name", "Product_Category", "Product_Department").distinct() \
    .withColumn("ProductKey", monotonically_increasing_id())
dim_product.write.format("delta").mode("overwrite").save("/silver/dim_product_orders")

# DIM_CUSTOMER
dim_customer = df_orders_shipments.select("Customer_ID", "Customer_Market", "Customer_Region", "Customer_Country").distinct() \
    .withColumn("CustomerKey", monotonically_increasing_id())
dim_customer.write.format("delta").mode("overwrite").save("/silver/dim_customer")

# DIM_LOCATION (customer/warehouse)
dim_location = df_orders_shipments.select("Customer_Country", "Customer_Region", "Warehouse_Country").distinct() \
    .withColumn("LocationKey", monotonically_increasing_id()) \
    .withColumn("Full_Location", concat_ws(", ", col("Customer_Region"), col("Customer_Country")))
dim_location.write.format("delta").mode("overwrite").save("/silver/dim_location")

# DIM_DATE (from Order_Date)
dim_date = df_orders_shipments.select("Order_Date").distinct() \
    .withColumn("DateKey", date_format(col("Order_Date"), "yyyyMMdd").cast(IntegerType())) \
    .withColumn("Year", year(col("Order_Date"))) \
    .withColumn("Month", month(col("Order_Date"))) \
    .withColumn("Day", dayofmonth(col("Order_Date"))) \
    .withColumn("Quarter", quarter(col("Order_Date")))
dim_date.write.format("delta").mode("overwrite").save("/silver/dim_date_orders")

# DIM_SHIPMENT_MODE
dim_shipment_mode = df_orders_shipments.select("Shipment_Mode").distinct() \
    .withColumn("ShipmentModeKey", monotonically_increasing_id()) \
    .withColumn("Description", col("Shipment_Mode"))
dim_shipment_mode.write.format("delta").mode("overwrite").save("/silver/dim_shipment_mode")

# COMMAND ----------

# ## FACT_ORDERS_SHIPMENTS

# COMMAND ----------

fact_orders_shipments = df_orders_shipments \
    .join(dim_product, df_orders_shipments.Product_Name == dim_product.Product_Name, "left") \
    .join(dim_customer, df_orders_shipments.Customer_ID == dim_customer.Customer_ID, "left") \
    .join(dim_date, df_orders_shipments.Order_Date == dim_date.Order_Date, "left") \
    .join(dim_location, (df_orders_shipments.Customer_Country == dim_location.Customer_Country) & (df_orders_shipments.Customer_Region == dim_location.Customer_Region), "left") \
    .join(dim_shipment_mode, df_orders_shipments.Shipment_Mode == dim_shipment_mode.Shipment_Mode, "left") \
    .select(
        col("dim_product.ProductKey"),
        col("dim_customer.CustomerKey"),
        col("dim_date.DateKey"),
        col("dim_location.LocationKey"),
        col("dim_shipment_mode.ShipmentModeKey"),
        col("Order_ID"),
        col("Order_Quantity"),
        col("Net_Sales"),
        col("Profit"),
        col("Delay_Shipment"),
        col("Shipping_Time")
    )
fact_orders_shipments.write.format("delta").mode("overwrite").save("/silver/fact_orders_shipments")

# COMMAND ----------

# ## Analysis and Insights

# COMMAND ----------

# Sales by department
spark.sql("""
    SELECT Product_Department, SUM(Net_Sales) as Total_Sales, AVG(Delay_Shipment) as Avg_Delay
    FROM orders_shipments_view
    GROUP BY Product_Department
""").show()

# Insights: Reduced from 30,871 rows post-cleaning; high delays in certain modes; sales trends by region; integrate with inventory for JIT optimization.
df_orders_shipments.unpersist()

```