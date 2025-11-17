# Databricks notebook source
# MAGIC %md
# MAGIC ## given part

# COMMAND ----------

# Test volume access
test_path = "/Volumes/workspace/default/2024_assignment-1_dic/"
dbutils.fs.ls(test_path)

# COMMAND ----------

# SOLUTION BLOCK (for reference - implement the TODOs above first!)
# Uncomment and run this cell if you need help completing the Bronze layer

"""
from pyspark.sql import functions as F

# Many CVE v5 exports have metadata under \"cveMetadata.datePublished\"
# Fall back defensively if a different field name appears
date_col = F.col(\"cveMetadata.datePublished\").cast(\"string\")
df_raw = df_raw.withColumn(\"_datePublished_ts\", F.to_timestamp(date_col))

# 2) Filter to 2024 publications
df_2024 = df_raw.filter(F.year(F.col(\"_datePublished_ts\")) == 2024)

# 3) Data quality gates
cnt_all = df_raw.count()
cnt_2024 = df_2024.count()
null_ids = df_2024.filter(F.col(\"cveMetadata.cveId\").isNull()).count()
distinct_ids = df_2024.select(\"cveMetadata.cveId\").distinct().count()

print(f\"Raw total: {cnt_all:,}\")
print(f\"2024 total: {cnt_2024:,}\")
print(f\"Null cveId (2024): {null_ids}\")
print(f\"Distinct cveId (2024): {distinct_ids:,}\")

assert cnt_2024 >= 30000, \"Too few 2024 rows—did the filter or read fail?\"
assert null_ids == 0, \"cveId should not be null\"
assert distinct_ids == cnt_2024, \"cveId expected to be unique in 2024\"

# 4) Write Delta + register table
(df_2024
 .repartition(8)
 .write
 .format(\"delta\")
 .mode(\"overwrite\")
 .save(bronze_path))

spark.sql(f\"DROP TABLE IF EXISTS {table_name}\")
spark.sql(f\"CREATE TABLE {table_name} USING DELTA LOCATION '{bronze_path}'\")

print(\"✅ Bronze Delta written and table registered:\", table_name)

# 5) Verification for screenshots
spark.read.format(\"delta\").load(bronze_path).printSchema()
spark.sql(f\"DESCRIBE DETAIL {table_name}\").show(truncate=False)
"""

# COMMAND ----------

# Step 3: Complete Bronze Layer Implementation
# YOUR TASK: Add the missing Bronze layer components below

from pyspark.sql import functions as F

# ---- Configuration (keep consistent with earlier cells) ----
volume_root = "/Volumes/workspace/default/assignment1"
volume_cves_dir = f"{volume_root}/cve/json_files"    # where JSONs were copied
bronze_path = "dbfs:/FileStore/cve/bronze"           # Delta target
table_name = "cve_bronze.records"

# Optimize for Community Edition
spark.conf.set("spark.sql.shuffle.partitions", "8")

print("🎯 YOUR TASK: Complete the Bronze layer implementation")
print("📋 Required: 1) 2024 filtering, 2) Delta write, 3) Table registration, 4) Quality checks")
print()

# 1) Read raw JSON recursively and add lineage
print("📖 Reading JSON files from volume...")
df_raw = (spark.read
          .option("multiLine", True)
          .option("recursiveFileLookup", "true")
          .json(volume_cves_dir)
          .withColumn("_source_file", F.input_file_name()))

print(f"✅ Raw CVE records loaded: {df_raw.count():,}")

# 2) TODO: Add 2024 filtering logic
# HINT: Use F.year(F.to_timestamp(F.col("cveMetadata.datePublished"))) == 2024
print("🚧 TODO: Add 2024 filtering by cveMetadata.datePublished")

# 3) TODO: Add data quality checks
# HINT: Check count >= 30000, null cveIds == 0, distinct IDs == count
print("🚧 TODO: Add data quality assertions")

# 4) TODO: Write Delta table
# HINT: Use .write.format("delta").mode("overwrite").save(bronze_path)
print("🚧 TODO: Write filtered data to Delta format")

# 5) TODO: Register table for SQL access
# HINT: CREATE TABLE cve_bronze.records USING DELTA LOCATION '...'
print("🚧 TODO: Register Delta table as cve_bronze.records")

# 6) TODO: Verification and screenshots
# HINT: Show schema, DESCRIBE DETAIL, and sample data
print("🚧 TODO: Add verification outputs for screenshots")

print()
print("📸 REQUIRED SCREENSHOTS:")
print("   • df_2024.count() showing ~40,000 records")  
print("   • DESCRIBE DETAIL cve_bronze.records output")
print("   • Data quality assertion results")
print("   • Delta files visible in path")

# COMMAND ----------

# MAGIC %md
# MAGIC # Assignment 1: Bronze Layer Example - CVE 2024 Data Ingestion
# MAGIC **DIC 587 - Data Intensive Computing - Fall 2025**
# MAGIC
# MAGIC This notebook implements the Bronze layer of our medallion architecture:
# MAGIC - Downloads CVEProject/cvelistV5 repository 
# MAGIC - Filters to 2024 vulnerabilities only (~40,000 records)
# MAGIC - Stores raw JSON as Delta tables with ACID guarantees
# MAGIC - Demonstrates streaming data ingestion patterns
# MAGIC
# MAGIC **Learning Objectives:**
# MAGIC - Understand Bronze layer concepts (raw data preservation)
# MAGIC - Practice JSON schema-on-read with PySpark
# MAGIC - Learn Delta Lake table registration
# MAGIC - Handle large-scale data downloads programmatically

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Environment Setup and Data Download

# COMMAND ----------

# MAGIC %md
# MAGIC only using the 2024 folder for this assignment

# COMMAND ----------

# Step 1: Configuration
from pyspark.sql import functions as F
import time

TARGET_YEAR = 2024
parquet_path = "/Volumes/workspace/default/dic_assignment-1_cve/2024_parquet.parquet"
bronze_path = "/Volumes/workspace/default/dic_assignment-1_cve/bronze"
table_name = "cve_bronze.records"

spark.conf.set("spark.sql.shuffle.partitions", "8")

print(f"Target year: {TARGET_YEAR}")
print(f"Parquet file: {parquet_path}")
print(f"Bronze path: {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Download CVE Repository
# MAGIC
# MAGIC This uploads the entire CVE catalog into a databricks catalog.  You need to figure out how to filter it down to the 2024 records.  You also need to do the other medallion letters!

# COMMAND ----------

# Read Parquet, Parse JSON, Filter, and Write to Delta

from pyspark.sql.functions import col, to_timestamp, year
import time

start_time = time.time()

# Read Parquet
print("Reading from Parquet...")
df_raw = spark.read.format("parquet").load(parquet_path)

# Convert to JSON format
temp_json_path = "/Volumes/workspace/default/dic_assignment-1_cve/temp_json"

print("Converting to JSON format...")
try:
    dbutils.fs.rm(temp_json_path, recurse=True)
except:
    pass

df_raw.select("json_data").write.mode("overwrite").text(temp_json_path)

# Read as JSON
print("Reading as JSON...")
parquet_read_start = time.time()

df_parsed = spark.read.json(temp_json_path)
raw_count = df_parsed.count()

parquet_read_time = time.time() - parquet_read_start
print(f"Loaded and parsed {raw_count:,} records in {parquet_read_time:.2f}s")

# Filter to 2024
print("Filtering to 2024...")
filter_start = time.time()

date_col = col("cveMetadata.datePublished").cast("string")
df_parsed = df_parsed.withColumn("_datePublished_ts", to_timestamp(date_col))
df_2024 = df_parsed.filter(year(col("_datePublished_ts")) == TARGET_YEAR)

# Data quality checks
cnt_2024 = df_2024.count()
null_ids = df_2024.filter(col("cveMetadata.cveId").isNull()).count()
distinct_ids = df_2024.select("cveMetadata.cveId").distinct().count()

filter_time = time.time() - filter_start

print(f"\nData Quality Checks:")
print(f"Total: {raw_count:,}")
print(f"2024 filtered: {cnt_2024:,}")
print(f"Null IDs: {null_ids}")
print(f"Distinct IDs: {distinct_ids:,}")

assert cnt_2024 >= 30000, f"Too few rows: {cnt_2024:,}"
assert null_ids == 0, "Null IDs found"
assert distinct_ids == cnt_2024, "IDs not unique"
print("Quality checks passed")

# Write Delta
print("\nWriting to Delta...")
delta_start = time.time()

try:
    dbutils.fs.rm(bronze_path, recurse=True)
except:
    pass

(df_2024
 .repartition(8)
 .write
 .format("delta")
 .mode("overwrite")
 .option("delta.columnMapping.mode", "name")
 .save(bronze_path))

delta_time = time.time() - delta_start
print(f"Delta write completed in {delta_time:.2f}s")

# Cleanup
print("Cleaning up temp files...")
try:
    dbutils.fs.rm(temp_json_path, recurse=True)
except:
    pass

total_time = time.time() - start_time
print(f"\nTiming: Parse+read {parquet_read_time:.2f}s | Filter {filter_time:.2f}s | Delta {delta_time:.2f}s | Total {total_time:.2f}s")
print(f"\nDelta files written to: {bronze_path}")
print(f"Records: {cnt_2024:,}")

# COMMAND ----------

# Cell 2: Register Delta Table with Column Mapping

# Read existing Delta table
df_bronze = spark.read.format("delta").load(bronze_path)

print(f"Delta table contains {df_bronze.count():,} records")

# Drop existing table
spark.sql("DROP TABLE IF EXISTS cve_bronze.records")

# Create table with column mapping using SQL on existing Delta location
spark.sql(f"""
CREATE TABLE cve_bronze.records
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS SELECT * FROM delta.`{bronze_path}`
""")

print("Table registered: cve_bronze.records")

# Verify
spark.sql("SELECT cveMetadata.cveId, cveMetadata.datePublished FROM cve_bronze.records LIMIT 5").show()
spark.sql("SELECT COUNT(*) as total_records FROM cve_bronze.records").show()

# COMMAND ----------

