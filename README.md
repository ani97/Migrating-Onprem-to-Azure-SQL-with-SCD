# Customer Account Data Pipeline Project

This project implements a **hybrid data pipeline** to ingest, transform, and analyze customer account data using Azure Data Factory, Azure SQL Database, and Power BI. The pipeline processes **on-premises data** with event-driven and scheduled triggers, applying **medallion architecture** (Bronze, Silver, Gold layers).

---

## 🚀 Architecture Overview

- **Scheduled Trigger** (5 AM daily): Initiates ingestion of on-prem files.
- **Self-Hosted IR**: Connects to on-premise data securely.
- **Key Vault Integration**: Manages secrets for secure connections.
- **Bronze Layer (Raw Storage)**: Raw files are ingested into ADLS Gen2.
- **Silver Layer (Cleaned Data)**: Event-triggered transformation, deduplication, and cleaning.
- **Gold Layer (Business-Ready)**: Data aggregated and loaded into Azure SQL for reporting.
- **Power BI Dashboards**: Business users consume insights from the Gold Layer.

---

## 🛠️ Components & Technologies

- **Azure Data Factory (ADF)**
- **Azure Data Lake Storage Gen2 (ADLS)**
- **Azure SQL Database**
- **Azure Key Vault**
- **Self-Hosted Integration Runtime (IR)**
- **Power BI**

---

## 📂 Folder Structure (Medallion Architecture)

```
project1/
├── bronze/         # Raw data from on-prem
├── silver/         # Cleaned and deduplicated data
├── gold/           # Business-ready data in SQL DB
├── backup/         # Backup of processed files
└── reject/         # Dirty records (nulls/invalid)
```


---

## 🔄 Pipeline Flow

### 1. **Ingestion Pipeline (Bronze Layer)**
- Triggered daily (5 AM) or via **Storage Event Trigger**.
- Uses **Self-Hosted IR** to connect to on-prem folders.
- **Metadata-driven**: Loops through subfolders and fetches **latest modified files** only.
- Moves files to `bronze/` layer and also creates a **dated backup**.

### 2. **Transformation Pipeline (Silver Layer)**
- **Triggered by Storage Event** when new files land in Bronze.
- Applies **deduplication** and **null-checks** via ADF **Data Flows**:
  - Deduplicate using **Aggregate** transformations.
  - Filter nulls and dirty records using **Conditional Split**.
- Clean records go to `silver/` layer.
- Rejected (dirty) records go to `reject/` folder.

### 3. **Load Pipeline (Gold Layer)**
- Triggered after Silver completion.
- Uses **SCD Type 1** and **SCD Type 2** logic via Data Flows to load into **Azure SQL Database**.
- **Hashing (CRC32)** is used for change detection in SCD flows.

---

## 🔑 SQL Schema (Azure SQL Database)

_Similar tables exist for customers (SCD 1), loans (SCD 1), loan_payments (SCD 2), and transactions (SCD 2)._

---

## ⚙️ Key Logic Implementations

### ✅ **Incremental File Processing**
- **Get Metadata + If Condition** filters files by `lastModified > watermark`.
- Watermark can be **trigger time** or a **config file/table**.

### ✅ **Data Cleaning in Silver**
- **Aggregate**: Remove duplicates.
- **Conditional Split**: Separate clean vs dirty records (null-check).

### ✅ **SCD 1 (Customers, Loans)**
- Compare **hashkeys** between source and target.
- **Insert** if new, **Update** if hash differs.

### ✅ **SCD 2 (Transactions, Loan Payments)**
- Insert new records.
- **Expire old records** (set `isActive=0`) and insert updated versions.

---

## 📊 Power BI Visualization

- Connects to **Azure SQL Database (Gold Layer)**.
- Dashboards display **Customer Account Analysis**.
- Published to **Fabric Workspace**.

---

## 📎 Features Summary

- 🔁 **Hybrid Orchestration**: Scheduled + Event-Driven triggers.
- 🔐 **Secure Connectivity**: Self-Hosted IR + Key Vault.
- 🗃 **Medallion Architecture**: Bronze, Silver, Gold layers.
- 🔄 **Incremental Loads**: Based on `lastModified` or watermark.
- 🧹 **Data Cleaning & Deduplication**: Null handling, reject path.
- 📈 **Power BI Integration**: Business-ready dashboards.

---

## 📬 Contact

_Aniket Gupta_

For questions, feel free to reach out via GitHub or LinkedIn!
