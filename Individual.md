You need individual PySpark code snippets for loading, cleaning, and transforming the **Inventory**, **Fulfillment**, and **Orders & Shipments** datasets into their respective Delta tables in the **Silver Layer**.

Below are the three distinct PySpark blocks for your $\text{PL\_LOAD\_SILVER\_LAYER}$ notebook, assuming the bronze tables already exist at $\text{/mnt/supplychain/bronze/}$.

-----

## 1\. Orders and Shipments Data (Core Fact Data)

This block handles the largest dataset, performing critical cleaning and feature engineering (dates, unit price, shipping delay).

```python
# --- Configuration ---
adls_root = "/mnt/supplychain"
bronze_path = f"{adls_root}/bronze"
silver_path = f"{adls_root}/silver"
spark.sql(f"SET spark.sql.shuffle.partitions = 8") # Optimization

# --- Ingestion ---
df_orders_bronze = spark.read.format("delta").load(f"{bronze_path}/orders_shipments_raw") 

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
    (col("Discount %") / 100).cast("double").alias("discount_percentage"),
    col("Profit").cast("double").alias("profit")
)
df_orders_silver.cache()

# --- Cleaning and Standardization ---
# 1. Duplicates & Null Filtering
df_orders_cleaned = df_orders_silver.dropDuplicates(['order_id', 'order_item_id']).filter(
    (col("order_id").isNotNull()) & 
    (col("customer_id").isNotNull()) & 
    (col("gross_sales").isNotNull()) &
    (col("order_quantity") > 0)
)

# 2. Categorical Standardization (InitCap and Trim)
string_cols_to_clean = ["product_department", "product_category", "product_name", 
                        "customer_market", "customer_region", "customer_country", 
                        "warehouse_country", "shipment_mode"]

for col_name in string_cols_to_clean:
    df_orders_cleaned = df_orders_cleaned.withColumn(
        col_name,
        when(col(col_name).isNull(), lit('Unknown')) 
        .otherwise(
            trim(regexp_replace(initcap(col(col_name)), r'[^\w\s]', ' '))
        )
    )

# --- Feature Creation ---
df_orders_transformed = df_orders_cleaned.withColumn(
    "order_date", to_date(concat_ws("-", col("order_year"), col("order_month"), col("order_day")), "y-M-d")
).withColumn(
    "order_datetime", to_timestamp(concat(col("order_date"), lit(" "), col("order_time_str")), "yyyy-MM-dd H:mm:ss")
).withColumn(
    "shipment_date", to_date(concat_ws("-", col("shipment_year"), col("shipment_month"), col("shipment_day")), "y-M-d")
)

df_orders_transformed = df_orders_transformed.withColumn(
    "shipping_time_days", datediff(col("shipment_date"), col("order_date"))
).withColumn(
    "delay_shipment",
    when(col("shipping_time_days") > col("scheduled_shipment_days"), lit("Late")).otherwise(lit("On Time"))
).withColumn(
    "unit_price",
    when(col("order_quantity") == 0, lit(0.0)).otherwise(col("gross_sales") / col("order_quantity"))
)

# --- Write to Silver Layer ---
df_orders_silver_final = df_orders_transformed.select(
    "order_id", "order_item_id", "customer_id", "order_date", "order_datetime", 
    "shipment_date", "order_quantity", "gross_sales", "discount_percentage", 
    "profit", "unit_price", "product_department", "product_category", "product_name", 
    "customer_market", "customer_region", "customer_country", "warehouse_country", 
    "shipment_mode", "scheduled_shipment_days", "shipping_time_days", "delay_shipment"
)

df_orders_silver_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}/silver_fact_orders_shipments")

print("Silver Layer: Orders and Shipments processing complete.")
```

-----

## 2\. Inventory Data

This block cleans the inventory data and standardizes the product name.

```python
# --- Configuration ---
adls_root = "/mnt/supplychain"
bronze_path = f"{adls_root}/bronze"
silver_path = f"{adls_root}/silver"

# --- Ingestion ---
df_inventory_bronze = spark.read.format("delta").load(f"{bronze_path}/inventory_raw")
df_inventory_silver = df_inventory_bronze.select(
    col("Product Name").alias("product_name"),
    col("Year Month").cast("integer").alias("inventory_year_month"),
    col("Warehouse Inventory").cast("integer").alias("warehouse_inventory"),
    col("Inventory Cost Per Unit").cast("double").alias("inventory_cost_per_unit")
)
df_inventory_silver.cache()

# --- Cleaning and Standardization ---
# 1. Duplicates & Null/Invalid Filtering
df_inventory_final = df_inventory_silver.filter(
    (col("warehouse_inventory").isNotNull()) & 
    (col("inventory_cost_per_unit").isNotNull()) &
    (col("warehouse_inventory") >= 0)
).dropDuplicates(['product_name', 'inventory_year_month'])

# 2. Product Name Standardization (matching orders/shipments)
df_inventory_final = df_inventory_final.withColumn(
    "product_name",
    trim(regexp_replace(initcap(col("product_name")), r'[^\w\s]', ' '))
)

# --- Write to Silver Layer ---
df_inventory_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}/silver_inventory")

print("Silver Layer: Inventory processing complete.")
```

-----

## 3\. Fulfillment Data

This block cleans the fulfillment data, ensuring valid duration days and standardizing the product name.

```python
# --- Configuration ---
adls_root = "/mnt/supplychain"
bronze_path = f"{adls_root}/bronze"
silver_path = f"{adls_root}/silver"

# --- Ingestion ---
df_fulfilment_bronze = spark.read.format("delta").load(f"{bronze_path}/fulfilment_raw")
df_fulfilment_silver = df_fulfilment_bronze.select(
    col("Product Name").alias("product_name"),
    col("Warehouse Order Fulfillment (days)").cast("double").alias("fulfillment_days")
)
df_fulfilment_silver.cache()

# --- Cleaning and Standardization ---
# 1. Duplicates & Null/Invalid Filtering
df_fulfilment_final = df_fulfilment_silver.filter(
    (col("fulfillment_days").isNotNull()) &
    (col("fulfillment_days") >= 0) # Fulfillment time shouldn't be negative
).dropDuplicates(['product_name'])

# 2. Product Name Standardization (matching orders/shipments)
df_fulfilment_final = df_fulfilment_final.withColumn(
    "product_name",
    trim(regexp_replace(initcap(col("product_name")), r'[^\w\s]', ' '))
)

# --- Write to Silver Layer ---
df_fulfilment_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}/silver_fulfilment")

print("Silver Layer: Fulfillment processing complete.")
```