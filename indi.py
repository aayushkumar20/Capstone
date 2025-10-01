# Read Silver for Gold processing
df_silver_orders_gold = spark.read.format("delta").load(f"{silver_path}/orders_silver")
df_silver_inventory_gold = spark.read.format("delta").load(f"{silver_path}/inventory_silver")
df_silver_fulfillment_gold = spark.read.format("delta").load(f"{silver_path}/fulfillment_silver")

# === DIMENSION TABLES (As many as possible for Power BI) ===

# 1. Product_DIM (SCD Type 1: product details)
df_product_dim = df_silver_orders_gold.select(
    col("product_name"),
    col("product_category"),
    col("product_department"),
    col("abcxyz_class"),
    col("unit_price").alias("avg_unit_price")  # Aggregated feature
).distinct().withColumn("product_key", monotonically_increasing_id())
df_product_dim.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_product")

# 2. Customer_DIM
df_customer_dim = df_silver_orders_gold.select(
    col("customer_id"),
    col("customer_market"),
    col("customer_region"),
    col("customer_country")
).distinct().withColumn("customer_key", monotonically_increasing_id())
df_customer_dim.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_customer")

# 3. Time_DIM (Granular: day-level for 2017-2018 range)
time_range = spark.range(0, 730).withColumn("date", date_add(lit(datetime(2017,1,1)), col("id")))  # Approx 2 years
df_time_dim = time_range.select(
    col("date"),
    year(col("date")).alias("year"),
    month(col("date")).alias("month"),
    dayofmonth(col("date")).alias("day"),
    quarter(col("date")).alias("quarter")
).withColumn("time_key", monotonically_increasing_id())
df_time_dim.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_time")

# 4. Location_DIM (Region/Country, include Warehouse)
df_location_dim = df_silver_orders_gold.select(
    col("customer_region"),
    col("customer_country"),
    col("warehouse_country")
).distinct().withColumn("location_key", monotonically_increasing_id())
df_location_dim.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_location")

# 5. Supplier_DIM (Inferred from Warehouse Country as proxy)
df_supplier_dim = df_silver_inventory_gold.select(
    col("warehouse_country").alias("supplier_country"),
    col("abcxyz_class")  # Proxy for supplier class
).distinct().withColumn("supplier_key", monotonically_increasing_id())
df_supplier_dim.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_supplier")

# 6. Shipment Mode_DIM
df_shipment_mode_dim = df_silver_orders_gold.select(
    col("shipment_mode"),
    col("shipment_days_scheduled").alias("avg_scheduled_days")
).distinct().withColumn("mode_key", monotonically_increasing_id())
df_shipment_mode_dim.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_shipment_mode")

# === FACT TABLES (Metrics at order/inventory granularity) ===

# 1. Sales_FACT (Order-level metrics)
df_sales_fact = df_silver_orders_gold.join(df_product_dim, df_silver_orders_gold.product_name == df_product_dim.product_name, "left") \
    .join(df_customer_dim, df_silver_orders_gold.customer_id == df_customer_dim.customer_id, "left") \
    .join(df_time_dim, df_silver_orders_gold.order_date == df_time_dim.date, "left") \
    .join(df_location_dim, (df_silver_orders_gold.customer_region == df_location_dim.customer_region) & 
          (df_silver_orders_gold.customer_country == df_location_dim.customer_country), "left") \
    .join(df_shipment_mode_dim, df_silver_orders_gold.shipment_mode == df_shipment_mode_dim.shipment_mode, "left") \
    .select(
        col("product_key"),
        col("customer_key"),
        col("time_key"),
        col("location_key"),
        col("mode_key"),
        col("order_id"),
        col("order_quantity"),
        col("gross_sales"),
        col("net_sales"),
        col("profit"),
        col("discount_pct"),
        col("is_delayed"),
        col("shipping_time")
    )

df_sales_fact.write.format("delta").mode("overwrite").partitionBy("time_key").save(f"{gold_path}/fact_sales")  # Partition by time for Power BI perf

# 2. Inventory_FACT (Monthly inventory snapshots)
df_inventory_fact = df_silver_inventory_gold.join(df_product_dim, df_silver_inventory_gold.product_name == df_product_dim.product_name, "left") \
    .join(df_time_dim, to_date(col("year_month"), "yyyy-MM") == df_time_dim.date, "left") \
    .join(df_location_dim, df_silver_inventory_gold.warehouse_country == df_location_dim.warehouse_country, "left") \
    .select(
        col("product_key"),
        col("time_key"),
        col("location_key"),
        col("warehouse_inventory"),
        col("inventory_cost_per_unit"),
        col("storage_cost")
    )

df_inventory_fact.write.format("delta").mode("overwrite").partitionBy("time_key").save(f"{gold_path}/fact_inventory")

# 3. Fulfillment_FACT (Product-level)
df_fulfillment_fact = df_silver_fulfillment_gold.join(df_product_dim, df_silver_fulfillment_gold.product_name == df_product_dim.product_name, "left") \
    .select(
        col("product_key"),
        col("fulfillment_days")
    )

df_fulfillment_fact.write.format("delta").mode("overwrite").save(f"{gold_path}/fact_fulfillment")

# Additional Features for Power BI (Calculated in Gold)
# E.g., Add to Sales_FACT: YoY growth (via DAX in Power BI, or here as view)
spark.sql("""
    CREATE OR REPLACE VIEW gold_sales_with_growth AS
    SELECT *, LAG(net_sales) OVER (PARTITION BY product_key ORDER BY time_key) AS prev_period_sales
    FROM fact_sales
""")

print("Gold layer loaded with star schema. DIMs: Product, Customer, Time, Location, Supplier, Shipment Mode. FACTs: Sales, Inventory, Fulfillment.")
print("In ADF: Trigger this notebook in 'PL LOAD GOLD_LAYER' after Silver load. Use Power BI to connect to Gold Delta tables for dashboards.")