# Databricks notebook source
# COMMAND ----------
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max, min
from pyspark.sql.types import StructType  # For schema definition if needed
import pyspark.sql.functions as F

# COMMAND ----------
# Initialize Spark session with Delta configurations
spark = SparkSession.builder \
    .appName("LoadSilverLayer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------
# Step: Add mounting point (assuming Azure Databricks)
# Replace with your actual storage details
storage_account_name = "yourstorageaccount"
container_name = "yourcontainer"
access_key = "your-access-key"  # Or use SAS token / managed identity

# Mount the storage (if not already mounted)
mount_point = "/mnt/data"
if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
        mount_point=mount_point,
        extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key}
    )
    print(f"Mounted {mount_point} successfully.")
else:
    print(f"{mount_point} is already mounted.")

# If mounting fails due to permissions, use direct abfss:// paths as alternative (comment out mount and uncomment below)
# bronze_csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/delta/csv/"
# bronze_json_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/delta/json/"
# silver_csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/csv/"
# silver_json_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/json/"

# COMMAND ----------
# Define paths using mount point
bronze_csv_path = f"{mount_point}/delta/csv/"
bronze_json_path = f"{mount_point}/delta/json/"
silver_csv_path = f"{mount_point}/silver/csv/"
silver_json_path = f"{mount_point}/silver/json/"

# COMMAND ----------
# Step 1: Define schema for the dataset (example schema; replace with actual based on your data)
# For CSV-derived data (assuming example columns: id, name, age, date)
csv_schema = StructType().add("id", "integer").add("name", "string").add("age", "integer").add("date", "date")

# For JSON-derived data (assuming example columns: key, value, timestamp)
json_schema = StructType().add("key", "string").add("value", "double").add("timestamp", "timestamp")

# Note: Since Bronze is Delta, schema is already enforced; this is for verification or if reading raw.

# COMMAND ----------
# Step 2: Read the data from Bronze Delta tables into DataFrames
# Read CSV Bronze Delta
df_csv = spark.read.format("delta").load(bronze_csv_path)
# Optionally enforce schema: df_csv = spark.read.format("delta").schema(csv_schema).load(bronze_csv_path)

# Read JSON Bronze Delta
df_json = spark.read.format("delta").load(bronze_json_path)
# Optionally enforce schema: df_json = spark.read.format("delta").schema(json_schema).load(bronze_json_path)

print("Data read from Bronze layer successfully.")

# COMMAND ----------
# Step 3: Verify the schema
print("CSV DataFrame Schema:")
df_csv.printSchema()

print("JSON DataFrame Schema:")
df_json.printSchema()

# COMMAND ----------
# Step 4: Check the datatypes (included in printSchema above; additional check)
csv_dtypes = df_csv.dtypes
json_dtypes = df_json.dtypes
print("CSV DataTypes:", csv_dtypes)
print("JSON DataTypes:", json_dtypes)

# COMMAND ----------
# Step 5: Cache the DataFrames for performance
df_csv.cache()
df_json.cache()
print("DataFrames cached.")

# COMMAND ----------
# Step 6: Verify the first few records
print("First 5 records of CSV DataFrame:")
df_csv.show(5, truncate=False)

print("First 5 records of JSON DataFrame:")
df_json.show(5, truncate=False)

# COMMAND ----------
# Step 7: Clean the data by removing duplicates, null values, and invalid data
# Remove duplicates
df_csv = df_csv.dropDuplicates()
df_json = df_json.dropDuplicates()

# Remove rows with null values (or fill; example: fill with defaults)
# df_csv = df_csv.na.drop()  # Drop rows with any nulls
# Or fill: df_csv = df_csv.na.fill({"age": 0, "name": "Unknown"})  # Example

# For invalid data (example: assume age > 0 for CSV)
if "age" in df_csv.columns:
    df_csv = df_csv.filter(col("age") > 0)

# Similar for JSON (example: value not null)
if "value" in df_json.columns:
    df_json = df_json.na.drop(subset=["value"])

print("Data cleaned: duplicates and invalid data removed.")

# COMMAND ----------
# Step 8: Check for data accuracy by validating against business rules
# Example business rules (customize based on your data):
# For CSV: Age between 18 and 100
if "age" in df_csv.columns:
    invalid_age_count = df_csv.filter(~col("age").between(18, 100)).count()
    print(f"Invalid age records in CSV: {invalid_age_count}")
    # Optionally filter out: df_csv = df_csv.filter(col("age").between(18, 100))

# For JSON: Value positive
if "value" in df_json.columns:
    invalid_value_count = df_json.filter(col("value") < 0).count()
    print(f"Invalid value records in JSON: {invalid_value_count}")

# COMMAND ----------
# Step 9: Ensure data is complete by checking for missing values and filling them
# Check missing values
csv_missing = df_csv.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_csv.columns])
print("Missing values in CSV:")
csv_missing.show()

json_missing = df_json.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_json.columns])
print("Missing values in JSON:")
json_missing.show()

# Fill missing (example)
# df_csv = df_csv.na.fill(0)  # Fill numerics with 0

# COMMAND ----------
# Step 10: Ensure data is consistent (datatypes already checked; format example: cast if needed)
# Example: Cast date to proper format
if "date" in df_csv.columns:
    df_csv = df_csv.withColumn("date", F.to_date(col("date"), "yyyy-MM-dd"))

# COMMAND ----------
# Step 11: Verify total number of rows and columns
csv_rows = df_csv.count()
csv_cols = len(df_csv.columns)
print(f"CSV DataFrame: {csv_rows} rows, {csv_cols} columns")

json_rows = df_json.count()
json_cols = len(df_json.columns)
print(f"JSON DataFrame: {json_rows} rows, {json_cols} columns")

# COMMAND ----------
# Step 12: Verify summary statistics
print("CSV Summary Statistics:")
df_csv.describe().show()

print("JSON Summary Statistics:")
df_json.describe().show()

# COMMAND ----------
# Step 13: Find maximum and minimum values in each column
# For numeric/date columns (example)
numeric_cols_csv = [c for c, t in df_csv.dtypes if t in ["int", "double", "float", "long"]]
if numeric_cols_csv:
    csv_min_max = df_csv.select([min(c).alias(f"min_{c}") for c in numeric_cols_csv] + [max(c).alias(f"max_{c}") for c in numeric_cols_csv])
    print("CSV Min/Max:")
    csv_min_max.show()

numeric_cols_json = [c for c, t in df_json.dtypes if t in ["int", "double", "float", "long"]]
if numeric_cols_json:
    json_min_max = df_json.select([min(c).alias(f"min_{c}") for c in numeric_cols_json] + [max(c).alias(f"max_{c}") for c in numeric_cols_json])
    print("JSON Min/Max:")
    json_min_max.show()

# COMMAND ----------
# Step 14: Find if there are any duplicate values in the columns
# Check duplicates across all columns (after dropDuplicates, should be 0)
csv_duplicates = df_csv.groupBy(df_csv.columns).count().filter("count > 1")
print("CSV Duplicates:")
csv_duplicates.show()

json_duplicates = df_json.groupBy(df_json.columns).count().filter("count > 1")
print("JSON Duplicates:")
json_duplicates.show()

# COMMAND ----------
# Step 15: Create a table/view on the Spark DataFrame to run SQL queries
df_csv.createOrReplaceTempView("csv_silver_view")
df_json.createOrReplaceTempView("json_silver_view")

# Example SQL query
spark.sql("SELECT * FROM csv_silver_view LIMIT 5").show()
spark.sql("SELECT * FROM json_silver_view LIMIT 5").show()

# COMMAND ----------
# Step 16: Complete preprocessing steps by bringing any required analysis and give insights
# Example analysis: Group by and count (customize based on columns)
# For CSV: Assume 'name' column
if "name" in df_csv.columns:
    csv_grouped = df_csv.groupBy("name").count().orderBy("count", ascending=False)
    print("CSV Insights: Top names by count")
    csv_grouped.show(10)

# For JSON: Assume 'key' column
if "key" in df_json.columns:
    json_grouped = df_json.groupBy("key").agg(F.avg("value").alias("avg_value"))
    print("JSON Insights: Average value by key")
    json_grouped.show(10)

# Additional insights: Total records, any anomalies from earlier checks
print("Insights: Data is now cleaned and at lowest granularity. Check for business-specific rules.")

# COMMAND ----------
# Write cleaned DataFrames to Silver layer as Delta tables
df_csv.write.format("delta").mode("overwrite").save(silver_csv_path)
df_json.write.format("delta").mode("overwrite").save(silver_json_path)

# Optional: Create Delta tables for querying
spark.sql(f"CREATE TABLE IF NOT EXISTS silver_csv_table USING DELTA LOCATION '{silver_csv_path}'")
spark.sql(f"CREATE TABLE IF NOT EXISTS silver_json_table USING DELTA LOCATION '{silver_json_path}'")

print("Data loaded to Silver layer successfully.")