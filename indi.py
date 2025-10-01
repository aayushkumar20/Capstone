# PL LOAD GOLD_LAYER Notebook (PySpark)

from pyspark.sql.functions import col, monotonically_increasing_id, year, month, dayofmonth, quarter, \
                                    date_add, lit, date_format, to_date, concat, avg, current_date, dayofweek
from datetime import datetime
from pyspark.sql.window import Window # For advanced Fact table features

# --- Configuration (Assumed) ---
silver_path = "/mnt/datalake/silver"
gold_path = "/mnt/datalake/gold"

print(f"Reading Silver Layer tables from: {silver_path}")
# --- Read Silver for Gold processing ---
df_silver_orders = spark.read.format("delta").load(f"{silver_path}/orders_silver")
df_silver_inventory = spark.read.format("delta").load(f"{silver_path}/inventory_silver")
df_silver_fulfillment = spark.read.format("delta").load(f"{silver_path}/fulfillment_silver")

# =========================================================================
# === DIMENSION TABLES (Enhanced for Power BI) ===
# =========================================================================
print("Creating Dimension Tables...")

# 1. DIM_Time (Correction: Use integer date key for deterministic join)
# Range from Jan 1, 2017 up to one year past current date for future-proofing
max_date = df_silver_orders.selectExpr("max(order_date)").collect()[0][0]
end_date = max_date if max_date else current_date()
time_range_days = (end_date.year - 2017) * 365 + end_date.dayofyear + 365

time_range = spark.range(0, time_range_days).withColumn("date", date_add(lit(datetime(2017,1,1)), col("id")))
df_time_dim = time_range.select(
    col("date"),
    concat(year(col("date")), date_format(col("date"), "MM"), dayofmonth(col("date"))).cast("int").alias("time_key"), # Corrected Key: YYYYMMDD
    year(col("date")).alias("year"),
    quarter(col("date")).alias("quarter"),
    month(col("date")).alias("month_of_year"),
    date_format(col("date"), "MMMM").alias("month_name"),
    dayofmonth(col("date")).alias("day_of_month"),
    dayofweek(col("date")).alias("day_of_week_num"),
    date_format(col("date"), "EEEE").alias("day_name"),
    when(dayofweek(col("date")).isin(1, 7), lit("Y")).otherwise(lit("N")).alias("is_weekend")
).distinct()
df_time_dim.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_time")
print("DIM_Time created with deterministic key and 10+ attributes.")


# 2. DIM_Product (SCD Type 1: product details + aggregated metrics)
# Use Product Name as the Natural Key
df_product_dim = df_silver_orders.groupBy(
    col("product_name"),
    col("product_category"),
    col("product_department")
).agg(
    avg(col("unit_price")).alias("avg_unit_price")
).withColumn("product_key", monotonically_increasing_id()) # Surrogate Key
df_product_dim.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_product")
print("DIM_Product created.")


# 3. DIM_Customer (Includes Market Segmentation)
df_customer_dim = df_silver_orders.select(
    col("customer_id"),
    col("customer_market"),
    col("customer_region"),
    col("customer_country")
).distinct().withColumn("customer_key", monotonically_increasing_id())
df_customer_dim.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_customer")
print("DIM_Customer created.")


# 4. DIM_Location (Separated Customer Location and Warehouse Location for better modeling)
df_location_dim = df_silver_orders.select(
    col("customer_region").alias("region"),
    col("customer_country").alias("country"),
    lit("Customer").alias("location_type")
).union(
    df_silver_orders.select(
        lit("N/A").alias("region"),
        col("warehouse_country").alias("country"),
        lit("Warehouse").alias("location_type")
    )
).distinct().withColumn("location_key", monotonically_increasing_id())
df_location_dim.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_location")
print("DIM_Location created with Customer and Warehouse types.")


# 5. DIM_Shipment (Enhanced Mode and Delay Status)
df_shipment_dim = df_silver_orders.select(
    col("shipment_mode"),
    col("shipment_days_scheduled").alias("scheduled_days"),
    col("delay_shipment").alias("is_delayed_flag") # A powerful boolean dimension
).distinct().withColumn("shipment_key", monotonically_increasing_id())
df_shipment_dim.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_shipment")
print("DIM_Shipment created.")


# 6. DIM_Supplier/Warehouse (Using Warehouse Country as proxy for supplier location)
df_supplier_dim = df_silver_inventory.select(
    col("product_category"), # Use a related attribute to differentiate suppliers
    col("product_department"),
    col("warehouse_country").alias("supplier_country")
).distinct().withColumn("supplier_key", monotonically_increasing_id())
df_supplier_dim.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_supplier")
print("DIM_Supplier created (Proxy).")


# =========================================================================
# === FACT TABLES (Metrics at lowest granularity) ===
# =========================================================================
print("\nCreating Fact Tables and joining keys...")

# 1. FACT_Sales (Order-level metrics)
df_sales_fact = df_silver_orders.alias("o") \
    .join(df_product_dim.alias("p"), col("o.product_name") == col("p.product_name"), "left") \
    .join(df_customer_dim.alias("c"), col("o.customer_id") == col("c.customer_id"), "left") \
    .join(df_time_dim.alias("t"), col("o.order_date") == col("t.date"), "left") \
    .join(df_location_dim.alias("lc"), (col("o.customer_country") == col("lc.country")) & (col("lc.location_type") == lit("Customer")), "left") \
    .join(df_location_dim.alias("lw"), (col("o.warehouse_country") == col("lw.country")) & (col("lw.location_type") == lit("Warehouse")), "left") \
    .join(df_shipment_dim.alias("s"), (col("o.shipment_mode") == col("s.shipment_mode")) & (col("o.delay_shipment") == col("s.is_delayed_flag")), "left") \
    .select(
        # Keys
        col("p.product_key"),
        col("c.customer_key"),
        col("t.time_key").alias("order_time_key"),
        col("lc.location_key").alias("customer_location_key"),
        col("lw.location_key").alias("warehouse_location_key"),
        col("s.shipment_key"),
        # Degenerate Dimension
        col("o.order_id"),
        # Measures (Facts)
        col("o.order_quantity").alias("fact_order_quantity"),
        col("o.gross_sales").alias("fact_gross_sales"),
        col("o.net_sales").alias("fact_net_sales"),
        col("o.profit").alias("fact_profit"),
        col("o.discount_pct").alias("fact_discount_percentage"),
        col("o.shipping_time").alias("fact_shipping_time_days")
    )

# Write FACT_Sales, partitioned by time_key for optimized Power BI query performance
df_sales_fact.write.format("delta").mode("overwrite").partitionBy("order_time_key").save(f"{gold_path}/fact_sales")
print("FACT_Sales created and partitioned by order_time_key.")


# 2. FACT_Inventory (Monthly inventory snapshots)
df_inventory_fact = df_silver_inventory.alias("i") \
    .join(df_product_dim.alias("p"), col("i.product_name") == col("p.product_name"), "left") \
    .join(df_supplier_dim.alias("sup"), (col("i.product_category") == col("sup.product_category")) & (col("i.warehouse_country") == col("sup.supplier_country")), "left") \
    .join(df_time_dim.alias("t"), to_date(concat(col("i.year_month"), lit("-01")), "yyyy-MM-dd") == col("t.date"), "left") \
    .select(
        # Keys
        col("p.product_key"),
        col("t.time_key").alias("snapshot_time_key"),
        col("sup.supplier_key"),
        # Measures (Facts)
        col("i.warehouse_inventory").alias("fact_warehouse_inventory_level"),
        col("i.inventory_cost_per_unit").alias("fact_inventory_cost_per_unit"),
        col("i.storage_cost").alias("fact_storage_cost")
    )

df_inventory_fact.write.format("delta").mode("overwrite").partitionBy("snapshot_time_key").save(f"{gold_path}/fact_inventory")
print("FACT_Inventory created and partitioned by snapshot_time_key.")


# 3. FACT_Fulfillment (Product-level metrics)
df_fulfillment_fact = df_silver_fulfillment.alias("f") \
    .join(df_product_dim.alias("p"), col("f.product_name") == col("p.product_name"), "left") \
    .select(
        col("p.product_key"),
        col("f.fulfillment_days").alias("fact_fulfillment_days")
    )

df_fulfillment_fact.write.format("delta").mode("overwrite").save(f"{gold_path}/fact_fulfillment")
print("FACT_Fulfillment created.")


# --- Additional Features for Power BI (SQL View for complex analysis) ---
print("\nCreating SQL View for advanced features (e.g., Year-over-Year calculations via SQL)")
# This view is a powerful addition for Power BI as it pre-calculates a window function metric
spark.sql(f"""
    CREATE OR REPLACE VIEW gold_sales_yoy_base AS
    SELECT 
        *, 
        LAG(fact_net_sales, 1) OVER (PARTITION BY product_key ORDER BY order_time_key) AS prev_sale_period_sales
    FROM delta.`{gold_path}/fact_sales`
""")
print("SQL View 'gold_sales_yoy_base' created for Power BI to easily calculate YoY growth using DAX.")
print("\n*** Gold Layer Load Complete. Ready for Power BI Connection. ***")