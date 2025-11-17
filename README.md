# CVE Lakehouse on Databricks

A Medallion Architecture implementation (Bronze → Silver → Gold) for analyzing 32,924 CVE vulnerabilities from 2024 using Databricks and Delta Lake.

## 📋 Overview

**Data Source**: [CVEProject/cvelistV5](https://github.com/CVEProject/cvelistV5)  
**Technologies**: Databricks, Delta Lake, PySpark, Unity Catalog, SQL  
**Architecture**: Bronze (raw) → Silver (normalized) → Gold (analytics)

---

## 🏗️ Architecture
```
2024 CVE JSON Files (38,727 records)
    ↓ [Local: convert_to_parquet.py]
Parquet File (39 MB)
    ↓ [Databricks: 01_ingest_cvelist.ipynb]
BRONZE: cve_bronze.records (32,924 records)
    ↓ [02_bronze_to_silver.ipynb]
SILVER: 
  ├─ cve_silver.core (32,924 records)
  └─ cve_silver.affected_products (61,255 records)
    ↓ [03_exploratory_analysis.ipynb]
GOLD: SQL Analytics & Visualizations
```

---

## 📁 Repository Structure
```
├── src_source                     # Source Files
├├── 01_ingest_cvelist.py
├├── 02_bronze_to_silver.py
└├──  03_exploratory_analysis.sql

├── src
├├── convert_to_parquet.py          # Local: JSON → Parquet conversion
├├── 01_ingest_cvelist.ipynb        # Bronze: Raw data ingestion & filtering
├├── 02_bronze_to_silver.ipynb      # Silver: Data normalization
└├── 03_exploratory_analysis.ipynb  # Gold: SQL analytics & visualizations

├── Screenshots
├├── bronze_ss
├├├── .png ... 
├├── silver_ss
├├├── .png ... 
└├── gold_ss
├├├── .png ... 
```
---

## 🚀 Quick Start

### 1. Local Data Preparation
```bash
# Download 2024 CVE data
git clone https://github.com/CVEProject/cvelistV5.git
cd cvelistV5/cves/2024

# Convert to Parquet (handles Databricks serverless limitations)
python convert_to_parquet.py
# Output: cve_2024_parquet/2024_parquet.parquet (39 MB, 38,727 records)

# Upload to Databricks volume: /Volumes/workspace/default/dic_assignment-1_cve/
```

### 2. Bronze Layer
Run `01_ingest_cvelist.ipynb`:
- Parse Parquet → JSON structure
- Filter by `cveMetadata.datePublished == 2024`
- Data quality checks (≥30k records, zero nulls, unique IDs)
- Write Delta with column mapping
- **Output**: `cve_bronze.records` (32,924 records)

### 3. Silver Layer
Run `02_bronze_to_silver.ipynb`:
- **Core table**: Extract metadata, CVSS scores (v3.1/v3.0/v2.0), descriptions
- **Affected products**: Explode vendor/product relationships
- **Outputs**: 
  - `cve_silver.core` (32,924 records)
  - `cve_silver.affected_products` (61,255 records, 5,978 vendors)

### 4. Gold Layer
Run `03_exploratory_analysis.ipynb`:
- Temporal trends (yearly, monthly, weekly)
- Risk distribution (Critical/High/Medium/Low)
- Top 25 vendors by CVE count
- Market concentration & seasonal patterns
- Databricks native visualizations

---

## 📊 Key Results

| Metric | Value |
|--------|-------|
| Total 2024 CVEs | 32,924 |
| Affected Products | 61,255 |
| Unique Vendors | 5,978 |
| Avg Publication Latency | 38.8 days |
| CVEs with CVSS Score | ~90% |

---

## 🎓 Key Technical Implementations

- **Delta Lake**: Column mapping for JSON special characters, ACID transactions
- **Data Quality**: Assertions for count thresholds, null checks, uniqueness validation
- **Schema-on-Read**: Complex nested JSON parsing, array explosion
- **Unity Catalog**: Volume operations, schema/table registration
- **Serverless Workaround**: Local Parquet conversion (RDD operations blocked)

---

## 📝 Configuration
```python
# Paths
volume_root = "/Volumes/workspace/default/dic_assignment-1_cve"
bronze_path = f"{volume_root}/bronze"
silver_core_path = f"{volume_root}/silver/core"
silver_affected_path = f"{volume_root}/silver/affected_products"

# Tables
bronze_table = "cve_bronze.records"
silver_core_table = "cve_silver.core"
silver_affected_table = "cve_silver.affected_products"
```




**Author**: Sreekar Govind | **Course**: DIC 587 - SUNY Buffalo | **Date**: November 2024

**Repository**: [github.com/sreekargovind27/cve_lakehouse_on_databricks](https://github.com/sreekargovind27/cve_lakehouse_on_databricks)
