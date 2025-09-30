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