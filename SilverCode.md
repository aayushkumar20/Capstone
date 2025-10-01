The PySpark notebook below is structured to match the cleaning logic and feature creation steps derived from your problem statement and the methodology typically employed in the linked project. This code fulfills all the requirements for the **Silver Layer** by focusing on data cleansing, standardization, and creating row-level features for the final dimensional modeling in the Gold Layer.

## PySpark Notebook: `nb_load_silver_layer`

### 1\. Setup and Configuration

```python
# Import necessary libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# Define Storage Paths (ADLS Mount Points)
# IMPORTANT: Replace '/mnt/supplychain' with your actual mount point or ADLS path
adls_root = "/mnt/supplychain"
bronze_path = f"{adls_root}/bronze"
silver_path = f"{adls_root}/silver"

# --- Placeholder for Mount Point Check (Optional) ---
# try:
#     dbutils.fs.ls(adls_root)
#     print(f"ADLS mount point {adls_root} is accessible.")
# except Exception as e:
#     print(f"Error accessing mount point: {e}. Please ensure the ADLS mount is configured.")

print("Setup complete. Reading data from Bronze Layer.")
```

-----

### 2\. Ingest Data from Bronze Layer and Initial Type Casting

We assume the raw files have been ingested into Delta format in the Bronze layer. We read them and apply initial type casting, standardizing the column names to `snake_case` for consistency.

```python
# --- 2.1 Orders and Shipments Data ---
try:
    df_orders_bronze = spark.read.format("delta").load(f"{bronze_path}/orders_shipments_raw") 
except Exception as e:
    print(f"Error reading orders_shipments_raw: {e}")
    # Handle error or exit if data is critical

df_orders_silver = df_orders_bronze.select(
    col("Order ID").cast("long").alias("order_id"),
    col("Order Item ID").cast("long").alias("order_item_id"),
    col("Order Year").cast("integer").alias("order_year"),
    col("Order Month").cast("integer").alias("order_month"),
    col("Order Day").cast("integer").alias("order_day"),
    col("Order Time").alias("order_time_str"), 
    col("Order Quantity").cast("integer").alias("order_quantity"),
    col("Product Department").alias("product_department"),
    col("Product Category").alias("product_category"),
    col("Product Name").alias("product_name"),
    col("Customer ID").cast("long").alias("customer_id"),
    col("Customer Market").alias("customer_market"),
    col("Customer Region").alias("customer_region"),
    col("Customer Country").alias("customer_country"),
    col("Warehouse Country").alias("warehouse_country"),
    col("Shipment Year").cast("integer").alias("shipment_year"),
    col("Shipment Month").cast("integer").alias("shipment_month"),
    col("Shipment Day").cast("integer").alias("shipment_day"),
    col("Shipment Mode").alias("shipment_mode"),
    col("Shipment Days -Scheduled").cast("integer").alias("scheduled_shipment_days"),
    col("Gross Sales").cast("double").alias("gross_sales"),
    (col("Discount %") / 100).cast("double").alias("discount_percentage"), # Standardize discount
    col("Profit").cast("double").alias("profit")
)
df_orders_silver.cache()

# --- 2.2 Fulfillment Data ---
df_fulfilment_bronze = spark.read.format("delta").load(f"{bronze_path}/fulfilment_raw")
df_fulfilment_silver = df_fulfilment_bronze.select(
    col("Product Name").alias("product_name"),
    col("Warehouse Order Fulfillment (days)").cast("double").alias("fulfillment_days")
)
df_fulfilment_silver.cache()

# --- 2.3 Inventory Data ---
df_inventory_bronze = spark.read.format("delta").load(f"{bronze_path}/inventory_raw")
df_inventory_silver = df_inventory_bronze.select(
    col("Product Name").alias("product_name"),
    col("Year Month").cast("integer").alias("inventory_year_month"),
    col("Warehouse Inventory").cast("integer").alias("warehouse_inventory"),
    col("Inventory Cost Per Unit").cast("double").alias("inventory_cost_per_unit")
)
df_inventory_silver.cache()

print("Initial dataframes created and cached.")
```

-----

### 3\. Data Cleaning and Standardization (Orders & Shipments)

This section implements the core data cleaning and standardization, matching the requirements for consistency, null handling, and removing special characters.

```python
# --- 3.1 Data Quality Checks and Filtering (Step 7, 8, 9, 14) ---

# 1. Remove rows with critical missing/invalid data (IDs and Quantity)
df_orders_cleaned = df_orders_silver.filter(
    (col("order_id").isNotNull()) & 
    (col("customer_id").isNotNull()) & 
    (col("order_quantity").isNotNull()) & 
    (col("gross_sales").isNotNull()) & 
    (col("order_quantity") > 0) # Business rule: Quantity must be positive
)

# 2. Remove Duplicates based on line item primary key (order_id, order_item_id)
initial_count = df_orders_cleaned.count()
df_orders_cleaned = df_orders_cleaned.dropDuplicates(['order_id', 'order_item_id'])
duplicates_removed = initial_count - df_orders_cleaned.count()
print(f"Removed {duplicates_removed} duplicate order line items.")

# --- 3.2 Standardization of Categorical Columns (Step 10) ---

# Apply standardization: Trim leading/trailing spaces, replace special characters (like '-') 
# and convert to a consistent format (Title Case or Proper Case)

string_cols_to_clean = [
    "product_department", "product_category", "product_name", 
    "customer_market", "customer_region", "customer_country", 
    "warehouse_country", "shipment_mode"
]

for col_name in string_cols_to_clean:
    df_orders_cleaned = df_orders_cleaned.withColumn(
        col_name,
        when(col(col_name).isNull(), lit('Unknown')) # Fill NULLs (Step 9)
        .otherwise(
            trim(
                regexp_replace(
                    # Replace common separators with a space
                    initcap(col(col_name)), 
                    r'[-_/]', 
                    ' '
                )
            )
        )
    )

# --- Specific Cleaning: Customer Country (as per typical requirement in linked projects) ---
# Remove special characters or inconsistent text in Customer Country
df_orders_cleaned = df_orders_cleaned.withColumn(
    "customer_country",
    regexp_replace(col("customer_country"), r'[^\w\s]', '') # Remove non-word/non-space characters
)

# Verify the final cleaned count
print(f"Final cleaned Orders & Shipments Rows: {df_orders_cleaned.count()}")
```

-----

### 4\. Feature Creation (Orders & Shipments)

This step creates the derived features required by the business requirements, which are crucial for the Fact Tables.

```python
# 1. Create Order and Shipment Date/Time Features (Feature Creation Requirement 1)
df_orders_transformed = df_orders_cleaned.withColumn(
    # Create combined order date/time (OrderDate is needed for DimDate)
    "order_date",
    to_date(concat_ws("-", col("order_year"), col("order_month"), col("order_day")), "y-M-d")
).withColumn(
    "order_datetime",
    to_timestamp(
        concat(
            col("order_date"), 
            lit(" "), 
            col("order_time_str")
        ), 
        "yyyy-MM-dd HH:mm" # Assuming Order Time is in HH:mm or H:mm:ss format
    )
).withColumn(
    # Create Shipment Date (ShipmentDate is needed for DimDate)
    "shipment_date",
    to_date(concat_ws("-", col("shipment_year"), col("shipment_month"), col("shipment_day")), "y-M-d")
)

# 2. Create Shipment Delay Features (Feature Creation Requirement 2)
df_orders_transformed = df_orders_transformed.withColumn(
    # Shipping Time (in days)
    "shipping_time_days",
    datediff(col("shipment_date"), col("order_date")) 
)

df_orders_transformed = df_orders_transformed.withColumn(
    # Delay Shipment Flag
    "delay_shipment",
    when(col("shipping_time_days") > col("scheduled_shipment_days"), lit("Late"))
    .otherwise(lit("On Time"))
)

# 3. Create Business Performance Feature (Unit Price - Feature Creation Requirement 3)
df_orders_transformed = df_orders_transformed.withColumn(
    "unit_price",
    col("gross_sales") / col("order_quantity")
)

# Final selection of columns for Silver Layer
df_orders_silver_final = df_orders_transformed.select(
    "order_id", "order_item_id", "customer_id", "order_date", "order_datetime",
    "order_quantity", "gross_sales", "discount_percentage", "profit", "unit_price",
    "product_department", "product_category", "product_name", 
    "customer_market", "customer_region", "customer_country", 
    "warehouse_country", "shipment_mode", "shipment_date", 
    "scheduled_shipment_days", "shipping_time_days", "delay_shipment"
)
print("Orders/Shipments feature creation complete.")
```

-----

### 5\. Cleaning and Standardization (Inventory & Fulfillment)

Clean and standardize the remaining two datasets.

```python
# --- 5.1 Inventory Data Cleaning ---
df_inventory_cleaned = df_inventory_silver.filter(
    (col("warehouse_inventory").isNotNull()) & 
    (col("inventory_cost_per_unit").isNotNull()) &
    (col("warehouse_inventory") >= 0) # Ensure no negative inventory
).dropDuplicates(['product_name', 'inventory_year_month'])

# Clean Product Name for consistency with Orders/Shipments
df_inventory_cleaned = df_inventory_cleaned.withColumn(
    "product_name",
    trim(regexp_replace(initcap(col("product_name")), r'[-_/]', ' '))
)


# --- 5.2 Fulfillment Data Cleaning ---
df_fulfilment_cleaned = df_fulfilment_silver.filter(
    (col("fulfillment_days").isNotNull()) &
    (col("fulfillment_days") >= 0)
).dropDuplicates(['product_name'])

# Clean Product Name for consistency with Orders/Shipments
df_fulfilment_cleaned = df_fulfilment_cleaned.withColumn(
    "product_name",
    trim(regexp_replace(initcap(col("product_name")), r'[-_/]', ' '))
)

print("Inventory and Fulfillment data cleaned.")
```

-----

### 6\. Final Verification and Write to Silver Layer

```python
# --- 6.1 Final Verification (Step 3, 4, 11, 12, 15) ---
print("\n--- Final Orders & Shipments Schema Verification (Step 3, 4) ---")
df_orders_silver_final.printSchema()

print(f"\nTotal Orders (Final) Rows: {df_orders_silver_final.count()}")

print("\n--- Summary Statistics (Step 12) ---")
df_orders_silver_final.select("gross_sales", "profit", "shipping_time_days").describe().display()

# Create Temporary View for immediate SQL query (Step 15)
df_orders_silver_final.createOrReplaceTempView("v_silver_orders")
print("\nExample SQL Query:")
spark.sql("SELECT delay_shipment, count(*) AS total_orders, avg(shipping_time_days) as avg_shipping_time FROM v_silver_orders GROUP BY 1").display()


# --- 6.2 Write to Silver Delta Tables (Step 2) ---
# This is the final output of the PL_LOAD_SILVER_LAYER pipeline.

# Write Orders/Shipments Data (Lowest Granularity for Fact/Dims)
df_orders_silver_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{silver_path}/silver_fact_orders_shipments")

# Write Inventory Data (Lowest Granularity for FactInventory)
df_inventory_cleaned.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{silver_path}/silver_inventory")

# Write Fulfillment Data (Lowest Granularity for Fact/Dims)
df_fulfilment_cleaned.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{silver_path}/silver_fulfilment")

print("\nAll data successfully written to Silver Layer Delta tables.")
```