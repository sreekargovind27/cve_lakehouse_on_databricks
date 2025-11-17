-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.functions import col, count, avg, sum as spark_sum, min as spark_min, max as spark_max, year, month, datediff, when, expr, round as spark_round
-- MAGIC from pyspark.sql.window import Window

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Temporal Analysis - Yearly CVE Counts

-- COMMAND ----------

SELECT 
    YEAR(date_published) as year,
    MONTH(date_published) as month,
    WEEKOFYEAR(date_published) as week,
    COUNT(*) as cve_count
FROM cve_silver.core
GROUP BY YEAR(date_published), MONTH(date_published), WEEKOFYEAR(date_published)
ORDER BY year, month, week

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Publication Latency Analysis 

-- COMMAND ----------

SELECT 
    ROUND(AVG(DATEDIFF(date_published, date_reserved)), 1) as avg_days_to_publish,
    MIN(DATEDIFF(date_published, date_reserved)) as min_days,
    MAX(DATEDIFF(date_published, date_reserved)) as max_days,
    PERCENTILE(DATEDIFF(date_published, date_reserved), 0.5) as median_days
FROM cve_silver.core
WHERE date_reserved IS NOT NULL 
  AND date_published IS NOT NULL
  AND DATEDIFF(date_published, date_reserved) >= 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Risk Distribution - CVSS Score Bucketing

-- COMMAND ----------

SELECT 
    CASE 
        WHEN cvss_base_score >= 9.0 THEN 'Critical'
        WHEN cvss_base_score >= 7.0 THEN 'High'
        WHEN cvss_base_score >= 4.0 THEN 'Medium'
        WHEN cvss_base_score > 0 THEN 'Low'
        ELSE 'Unknown'
    END as severity,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM cve_silver.core
GROUP BY severity
ORDER BY 
    CASE severity 
        WHEN 'Critical' THEN 1 
        WHEN 'High' THEN 2 
        WHEN 'Medium' THEN 3 
        WHEN 'Low' THEN 4 
        ELSE 5 
    END

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. CVSS Score Statistics 

-- COMMAND ----------

SELECT 
    COUNT(*) as total_scored,
    ROUND(AVG(cvss_base_score), 2) as avg_score,
    ROUND(MIN(cvss_base_score), 2) as min_score,
    ROUND(MAX(cvss_base_score), 2) as max_score,
    PERCENTILE(cvss_base_score, 0.5) as median_score
FROM cve_silver.core
WHERE cvss_base_score IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Top 25 Vendors by Vulnerability Count

-- COMMAND ----------

SELECT 
    vendor, 
    COUNT(DISTINCT cve_id) as cve_count
FROM cve_silver.affected_products
WHERE vendor IS NOT NULL
GROUP BY vendor
ORDER BY cve_count DESC
LIMIT 25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Market Concentration Analysis 

-- COMMAND ----------

SELECT 
    vendor, 
    COUNT(DISTINCT cve_id) as cve_count
FROM cve_silver.affected_products
WHERE vendor IS NOT NULL
GROUP BY vendor
ORDER BY cve_count DESC
LIMIT 25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Monthly Trend Analysis (2024)

-- COMMAND ----------

SELECT 
    MONTH(date_published) as month,
    COUNT(*) as cve_count,
    ROUND(AVG(cvss_base_score), 2) as avg_cvss_score
FROM cve_silver.core
WHERE YEAR(date_published) = 2024 AND cvss_base_score IS NOT NULL
GROUP BY month
ORDER BY month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Seasonal Patterns

-- COMMAND ----------

SELECT 
    CASE 
        WHEN MONTH(date_published) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(date_published) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(date_published) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END as season,
    COUNT(*) as cve_count,
    ROUND(AVG(cvss_base_score), 2) as avg_severity
FROM cve_silver.core
WHERE cvss_base_score IS NOT NULL
GROUP BY season
ORDER BY 
    CASE season 
        WHEN 'Winter' THEN 1 
        WHEN 'Spring' THEN 2 
        WHEN 'Summer' THEN 3 
        ELSE 4 
    END

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Unknown/Unscored Vulnerability Identification

-- COMMAND ----------

SELECT 
    COUNT(*) as total_cves,
    SUM(CASE WHEN cvss_base_score IS NULL THEN 1 ELSE 0 END) as unscored_cves,
    ROUND(SUM(CASE WHEN cvss_base_score IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as unscored_percentage
FROM cve_silver.core

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. Vendor-Specific Risk Profiles (Top 10)

-- COMMAND ----------

SELECT 
    a.vendor,
    COUNT(DISTINCT a.cve_id) as total_cves,
    ROUND(AVG(c.cvss_base_score), 2) as avg_cvss,
    SUM(CASE WHEN c.cvss_base_score >= 9.0 THEN 1 ELSE 0 END) as critical_count,
    SUM(CASE WHEN c.cvss_base_score >= 7.0 AND c.cvss_base_score < 9.0 THEN 1 ELSE 0 END) as high_count
FROM cve_silver.affected_products a
JOIN cve_silver.core c ON a.cve_id = c.cve_id
WHERE a.vendor IS NOT NULL AND c.cvss_base_score IS NOT NULL
GROUP BY a.vendor
ORDER BY total_cves DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 11. Summary Statistics

-- COMMAND ----------

SELECT 
    COUNT(*) as total_cves,
    COUNT(DISTINCT YEAR(date_published)) as years_covered,
    COUNT(cvss_base_score) as scored_cves,
    ROUND(AVG(cvss_base_score), 2) as avg_cvss,
    MIN(date_published) as earliest_date,
    MAX(date_published) as latest_date
FROM cve_silver.core