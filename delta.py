from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ConvertRawToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define mount point paths
raw_csv_path = "/mnt/raw/csv/"  # Mounted path for CSV files
raw_json_path = "/mnt/raw/json/"  # Mounted path for JSON files
delta_csv_path = "/mnt/delta/csv/"
delta_json_path = "/mnt/delta/json/"

# Read CSV files
df_csv = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(raw_csv_path)

# Write as Delta table
df_csv.write.format("delta") \
    .mode("overwrite") \
    .save(delta_csv_path)

# Read JSON files
df_json = spark.read.format("json") \
    .load(raw_json_path)

# Write as Delta table
df_json.write.format("delta") \
    .mode("overwrite") \
    .save(delta_json_path)

# Optional: Create Delta tables for SQL querying
spark.sql(f"CREATE TABLE IF NOT EXISTS delta_csv_table USING DELTA LOCATION '{delta_csv_path}'")
spark.sql(f"CREATE TABLE IF NOT EXISTS delta_json_table USING DELTA LOCATION '{delta_json_path}'")

print("Conversion to Delta format completed.")