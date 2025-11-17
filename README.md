# CVE Lakehouse on Databricks

A complete implementation of the Medallion Architecture (Bronze → Silver → Gold) for CVE (Common Vulnerabilities and Exposures) data analysis on Databricks, built as part of the DIC 587 - Data Intensive Computing course.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Setup & Installation](#setup--installation)
- [Data Pipeline](#data-pipeline)
- [Key Learnings](#key-learnings)
- [Results](#results)

---

## 🎯 Overview

This project implements a modern data lakehouse architecture to process and analyze **32,924 CVE vulnerability records from 2024**, demonstrating:

- **Bronze Layer**: Raw data ingestion with ACID guarantees
- **Silver Layer**: Data normalization and quality enhancement
- **Gold Layer**: Business-ready analytics and insights

**Data Source**: [CVEProject/cvelistV5](https://github.com/CVEProject/cvelistV5) - Official CVE vulnerability database

**Key Technologies**: Databricks, Delta Lake, PySpark, Unity Catalog, SQL

---

## 🏗️ Architecture

### Medallion Architecture Layers
```
Raw JSON (2024 folder)
    ↓ [Local Conversion]
Parquet File (38,727 records)
    ↓ [Upload to Databricks]
┌─────────────────────────────────────────────────────────────┐
│ BRONZE LAYER                                                │
│ - Raw CVE JSON data preserved                               │
│ - Filtered to 2024 (32,924 records)                         │
│ - Delta Lake format with column mapping                     │
│ - Table: cve_bronze.records                                 │
└─────────────────────────────────────────────────────────────┘
    ↓ [Normalize & Extract]
┌─────────────────────────────────────────────────────────────┐
│ SILVER LAYER                                                │
│ - Core CVE Table: 32,924 records                            │
│   • Metadata, CVSS scores, descriptions                     │
│ - Affected Products Table: 61,255 records                   │
│   • Exploded vendor/product relationships                   │
│ - Tables: cve_silver.core, cve_silver.affected_products     │
└─────────────────────────────────────────────────────────────┘
    ↓ [Aggregate & Analyze]
┌─────────────────────────────────────────────────────────────┐
│ GOLD LAYER                                                  │
│ - SQL Analytics & Visualizations                            │
│ - Risk distribution analysis                                │
│ - Vendor vulnerability trends                               │
│ - Temporal patterns (monthly, seasonal)                     │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 Repository Structure
```
cve_lakehouse_on_databricks/
│
├── convert_to_parquet.py          # Local script to convert 2024 CVE JSON to Parquet
│
├── 01_ingest_cvelist.ipynb        # Bronze Layer: Data ingestion
│   ├── Configuration & setup
│   ├── Read Parquet → Parse JSON
│   ├── Filter to 2024 by datePublished
│   ├── Data quality checks (count, nulls, uniqueness)
│   ├── Write Delta Lake with column mapping
│   └── Register table: cve_bronze.records
│
├── 02_bronze_to_silver.ipynb      # Silver Layer: Data normalization
│   ├── Core CVE table creation
│   │   └── Extract metadata, CVSS scores, descriptions
│   ├── Affected Products table (exploded)
│   │   └── Vendor/product relationships
│   └── Register tables: cve_silver.core, cve_silver.affected_products
│
└── 03_exploratory_analysis.ipynb  # Gold Layer: SQL analytics
    ├── Temporal analysis (yearly, monthly, weekly)
    ├── Risk distribution (CVSS severity bucketing)
    ├── Top 25 vendors by vulnerability count
    ├── Market concentration analysis
    ├── Seasonal patterns
    ├── Vendor-specific risk profiles
    └── Databricks visualizations
```

---

## ⚙️ Prerequisites

### Local Environment (for data preparation)
- Python 3.8+
- Required packages:
```bash
  pip install pyarrow pandas
```

### Databricks Environment
- Databricks workspace with Unity Catalog enabled
- Cluster with:
  - Databricks Runtime 13.3 LTS or higher
  - Python 3
  - Access to Unity Catalog volumes
- Permissions:
  - CREATE SCHEMA
  - CREATE TABLE
  - USE CATALOG

---

## 🚀 Setup & Installation

### Step 1: Local Data Preparation

1. **Download 2024 CVE data** from the [CVEProject repository](https://github.com/CVEProject/cvelistV5)
```bash
   # Clone the repository
   git clone https://github.com/CVEProject/cvelistV5.git
   
   # Navigate to 2024 folder
   cd cvelistV5/cves/2024
```

2. **Convert JSON to Parquet** (handles Databricks serverless limitations)
```bash
   # Run the conversion script
   python convert_to_parquet.py
```
   
   **Output**: 
   - File: `cve_2024_parquet/2024_parquet.parquet`
   - Size: ~39 MB
   - Records: 38,727 JSON strings

3. **Upload to Databricks**
   - Create Unity Catalog volume: `/Volumes/workspace/default/dic_assignment-1_cve/`
   - Upload `2024_parquet.parquet` file

### Step 2: Bronze Layer (Raw Data Ingestion)

Run `01_ingest_cvelist.ipynb`:
- Reads Parquet file
- Parses JSON strings to structured DataFrame
- Filters to 2024 by `cveMetadata.datePublished`
- Validates data quality:
  - ✅ Count ≥ 30,000 (got 32,924)
  - ✅ Zero null CVE IDs
  - ✅ Unique CVE IDs
- Writes Delta Lake with column mapping enabled
- Registers `cve_bronze.records` table

### Step 3: Silver Layer (Data Normalization)

Run `02_bronze_to_silver.ipynb`:

**Core CVE Table**:
- Explodes metrics array to extract CVSS scores (v3.1, v3.0, v2.0)
- Extracts metadata: CVE ID, dates, state, descriptions
- Deduplicates by CVE ID
- Creates `cve_silver.core` (32,924 records)

**Affected Products Table**:
- Explodes `containers.cna.affected` array
- Creates vendor/product relationships
- Creates `cve_silver.affected_products` (61,255 records)

### Step 4: Gold Layer (Analytics)

Run `03_exploratory_analysis.ipynb`:
- SQL-based analytics queries
- Databricks native visualizations
- Insights on risk distribution, vendor trends, temporal patterns

---

## 📊 Data Pipeline

### Bronze Layer Details
- **Input**: 38,727 JSON records (all 2024 CVE data)
- **Filter**: `year(cveMetadata.datePublished) == 2024`
- **Output**: 32,924 records (some records have null/invalid dates)
- **Storage**: `/Volumes/workspace/default/dic_assignment-1_cve/bronze/`
- **Format**: Delta Lake with column mapping (handles special characters in JSON keys)

### Silver Layer Details

**Core Table Schema**:
```
cve_id: string
state: string
date_published: timestamp
date_reserved: timestamp
date_updated: timestamp
cvss_base_score: double
cvss_severity: string
description: string
```

**Affected Products Schema**:
```
cve_id: string
vendor: string
product: string
default_status: string
```

### Gold Layer Analytics

Key analyses performed:
1. **Temporal Trends**: CVE counts by year, month, week
2. **Risk Distribution**: Critical (≥9.0), High (≥7.0), Medium (≥4.0), Low (>0)
3. **Vendor Analysis**: Top 25 vendors, market concentration
4. **Seasonal Patterns**: Winter, Spring, Summer, Fall CVE trends
5. **CVSS Statistics**: Average, median, min, max scores

---

## 🎓 Key Learnings

### Technical Skills Demonstrated

1. **Databricks Serverless Workarounds**
   - Converted JSON to Parquet locally (RDD operations blocked)
   - Used intermediate text files for JSON parsing
   
2. **Delta Lake Features**
   - Column mapping for special characters
   - ACID transactions
   - Time travel capabilities
   
3. **Data Quality Engineering**
   - Assertions for count thresholds
   - Null validation
   - Uniqueness checks
   
4. **Schema-on-Read**
   - Handled complex nested JSON structures
   - Exploded arrays for normalization
   - Coalesced multiple CVSS versions

5. **Unity Catalog**
   - Volume operations with `dbutils.fs`
   - Schema and table registration
   - Managed tables with TBLPROPERTIES

---

## 📈 Results

### Data Summary

| Layer | Records | Key Metrics |
|-------|---------|-------------|
| Bronze | 32,924 CVEs | 100% 2024 data, zero nulls |
| Silver (Core) | 32,924 records | CVSS scores extracted |
| Silver (Affected) | 61,255 records | 5,978 unique vendors |
| Gold | Analytics | 10+ SQL queries |

### Key Insights

- **Risk Distribution**: 
  - Critical vulnerabilities: ~X%
  - High severity: ~Y%
  - Most CVEs in Medium range
  
- **Top Vendors**: 
  - Google, Microsoft, Linux lead in CVE counts
  - Top 10 vendors account for ~X% of all CVEs
  
- **Temporal Patterns**:
  - Average publication latency: 38.8 days
  - Peak months: [Based on analysis]
  - Seasonal variations observed

---

## 🔧 Configuration Notes

### Paths Used
```python
volume_root = "/Volumes/workspace/default/dic_assignment-1_cve"
bronze_path = f"{volume_root}/bronze"
silver_core_path = f"{volume_root}/silver/core"
silver_affected_path = f"{volume_root}/silver/affected_products"
```

### Table Names
```python
bronze_table = "cve_bronze.records"
silver_core_table = "cve_silver.core"
silver_affected_table = "cve_silver.affected_products"
```

---

## 📝 Assignment Requirements Met

✅ Bronze Layer: Raw data ingestion with 2024 filtering  
✅ Silver Layer: Normalized core and affected products tables  
✅ Gold Layer: SQL analytics with visualizations  
✅ Data Quality: Assertions and validation checks  
✅ Delta Lake: ACID transactions and column mapping  
✅ Unity Catalog: Schema and table registration  
✅ Documentation: Complete README and code comments  

---

## 🤝 Contributing

This project was built for academic purposes as part of DIC 587 - Data Intensive Computing.

**Author**: Sreekar Govind  
**Course**: DIC 587 - Fall 2025  
**Institution**: SUNY Buffalo

---

## 📄 License

This project is for educational purposes only.

---

## 🔗 References

- [CVE Project Repository](https://github.com/CVEProject/cvelistV5)
- [Databricks Delta Lake Documentation](https://docs.databricks.com/delta/)
- [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

---

**Last Updated**: November 2024