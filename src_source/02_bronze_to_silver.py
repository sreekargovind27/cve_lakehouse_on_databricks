# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, explode_outer
import matplotlib.pyplot as plt
import pandas as pd

spark.sql("CREATE SCHEMA IF NOT EXISTS cve_silver")

volume_root = "/Volumes/workspace/default/dic_assignment-1_cve"
bronze_table = "cve_bronze.records"
silver_core_path = f"{volume_root}/silver/core"
silver_affected_path = f"{volume_root}/silver/affected_products"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Core CVE Table

# COMMAND ----------

import time
start = time.time()

df_bronze = spark.table(bronze_table)

df_with_metrics = df_bronze.withColumn("metric", explode_outer(col("containers.cna.metrics")))

df_core = df_with_metrics.select(
    col("cveMetadata.cveId").alias("cve_id"),
    col("cveMetadata.state").alias("state"),
    col("cveMetadata.datePublished").cast("timestamp").alias("date_published"),
    col("cveMetadata.dateReserved").cast("timestamp").alias("date_reserved"),
    col("cveMetadata.dateUpdated").cast("timestamp").alias("date_updated"),
    coalesce(
        col("metric.cvssV3_1.baseScore"),
        col("metric.cvssV3_0.baseScore"),
        col("metric.cvssV2_0.baseScore")
    ).alias("cvss_base_score"),
    coalesce(
        col("metric.cvssV3_1.baseSeverity"),
        col("metric.cvssV3_0.baseSeverity")
    ).alias("cvss_severity"),
    col("containers.cna.descriptions").getItem(0).getField("value").alias("description")
).dropDuplicates(["cve_id"])

try:
    dbutils.fs.rm(silver_core_path, recurse=True)
except:
    pass

df_core.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(silver_core_path)

spark.sql("DROP TABLE IF EXISTS cve_silver.core")
spark.sql(f"CREATE TABLE cve_silver.core USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name') AS SELECT * FROM delta.`{silver_core_path}`")

print(f"Core table created: {df_core.count():,} records in {time.time()-start:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Core Table

# COMMAND ----------

spark.sql("SELECT cve_id, date_published, cvss_base_score, cvss_severity FROM cve_silver.core WHERE cvss_base_score IS NOT NULL LIMIT 10").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Affected Products Table

# COMMAND ----------

import time
start = time.time()

df_bronze = spark.table(bronze_table)

df_affected = df_bronze.select(
    col("cveMetadata.cveId").alias("cve_id"),
    explode_outer(col("containers.cna.affected")).alias("affected")
).select(
    col("cve_id"),
    col("affected.vendor").alias("vendor"),
    col("affected.product").alias("product"),
    col("affected.defaultStatus").alias("default_status")
).filter(col("vendor").isNotNull() | col("product").isNotNull())

try:
    dbutils.fs.rm(silver_affected_path, recurse=True)
except:
    pass

df_affected.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(silver_affected_path)

spark.sql("DROP TABLE IF EXISTS cve_silver.affected_products")
spark.sql(f"CREATE TABLE cve_silver.affected_products USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name') AS SELECT * FROM delta.`{silver_affected_path}`")

print(f"Affected products table created: {df_affected.count():,} records in {time.time()-start:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Affected Products

# COMMAND ----------

spark.sql("SELECT cve_id, vendor, product FROM cve_silver.affected_products LIMIT 10").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

core_count = spark.table("cve_silver.core").count()
affected_count = spark.table("cve_silver.affected_products").count()
unique_vendors = spark.sql("SELECT COUNT(DISTINCT vendor) FROM cve_silver.affected_products WHERE vendor IS NOT NULL").collect()[0][0]

print(f"Silver Layer Summary:")
print(f"  Core CVE records: {core_count:,}")
print(f"  Affected product records: {affected_count:,}")
print(f"  Unique vendors: {unique_vendors:,}")