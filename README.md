# databricks-lakehouse-etl-pipeline
<img width="1408" height="768" alt="image" src="https://github.com/user-attachments/assets/c2c11af1-bbc6-4eac-9528-7b48bf0c8fc1" />

## Project Architecture

This project demonstrates an end-to-end **Lakehouse ETL pipeline** built on **Azure Databricks** using the **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline ingests raw data from Azure Data Lake Storage Gen2 and processes it using **PySpark and Delta Live Tables** to create structured analytical datasets.

### Data Flow Overview

1. **Source Data (Azure Data Lake Gen2)**  
   Raw data files are stored in Azure Data Lake Storage in Parquet format.

2. **Bronze Layer – Raw Ingestion**  
   Databricks **Autoloader** ingests raw files into Delta tables while preserving the original structure of the data.

3. **Silver Layer – Data Cleaning & Transformation**  
   PySpark transformations are applied to clean, validate, and standardize the data.

4. **Gold Layer – Data Modeling**  
   Delta Live Tables are used to create **Star Schema models** including:
   - Dimension tables
   - Fact tables

5. **Data Warehouse Layer**  
   Curated datasets are structured for analytical workloads.

6. **Reporting Layer**  
   The final data model can be used by BI tools such as **Power BI** for reporting and dashboard creation.

7. **Version Control (GitHub)**  
   All pipeline notebooks and scripts are version controlled using GitHub.

### Key Technologies Used

- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Databricks Autoloader
- Delta Lake
- Delta Live Tables
- GitHub (Version Control)

- ## Bronze Layer – Raw Data Ingestion

The Bronze layer is responsible for ingesting raw source data into the Lakehouse.

In this project, raw data is stored as **Parquet files in Azure Data Lake Storage Gen2** and ingested using **Databricks Autoloader**.

Autoloader enables efficient and scalable ingestion of files by automatically detecting new files added to the source storage.

### Key Features

- Incremental file ingestion using **Databricks Autoloader**
- Schema inference and storage of schema metadata
- Raw data preservation for audit and reprocessing
- Data stored in **Delta Lake format**
- Checkpointing enabled for fault tolerance

### Implementation Details

The ingestion pipeline reads data from the source container and writes it into the Bronze container using a streaming job.

Autoloader configuration:

- Source format: **Parquet**
- Streaming ingestion using **cloudFiles**
- Schema stored in a separate schema location
- Checkpoints used for maintaining streaming state

- ## Silver Layer – Data Cleaning & Transformation

The Silver layer focuses on transforming and enriching the raw data ingested in the Bronze layer. In this stage, the raw Delta tables are cleaned, standardized, and enhanced using PySpark transformations to prepare them for analytical modeling in the Gold layer.

Key transformations performed in this layer include:

• **Data Cleaning**
- Removed unnecessary columns such as `_rescued_data` generated during ingestion.
- Ensured structured and usable schema for downstream processing.

• **Data Enrichment**
- Derived new analytical columns such as `year` from order timestamps.
- Created `discounted_price` for products using a custom discount function.
- Extracted email domains from customer emails for analysis.

• **Feature Engineering**
- Generated ranking indicators (`dense_rank`, `row_number`) on order data using Spark window functions to enable analytical insights.
- Created a `full_name` column by combining first and last names for customer datasets.

• **Aggregation and Analytical Preparation**
- Performed domain-level aggregation of customers for basic analytical insights.
- Prepared datasets suitable for dimensional modeling.

• **Delta Table Storage**
- Cleaned and transformed data was written into **Delta tables in the Silver layer** within Azure Data Lake Storage Gen2 for efficient querying and downstream processing.

This layer ensures that data moving into the Gold layer is **clean, enriched, and structured for building analytical models such as star schemas and fact tables.**

## Gold Layer – Dimensional Modeling & Analytics

The Gold layer represents the final curated layer of the Lakehouse architecture where cleaned and enriched datasets from the Silver layer are transformed into analytical data models.

In this stage, the data is structured using a **Star Schema design** consisting of **dimension tables and fact tables** to support efficient analytical queries and reporting.

### Key Processing Steps

• **Dimension Table Creation**

Dimension tables are built to provide descriptive attributes for analytical queries.

- **DimCustomers**
  - Duplicate customer records are removed.
  - Surrogate keys are generated using Spark functions.
  - Metadata columns such as `create_date` and `update_date` are added.
  - Incremental updates are handled using **Delta Lake MERGE operations** to maintain up-to-date customer records.

- **DimProducts**
  - Implemented using **Delta Live Tables (DLT)** for automated pipeline management.
  - Data quality rules are enforced to ensure critical fields such as `product_id` and `product_name` are not null.
  - Change data is handled using **Slowly Changing Dimension (SCD Type-2)** logic to track historical product changes.
  - 

### Fact Table Creation

The **FactOrders** table is created by joining transactional order data with the corresponding dimension tables.

Key steps include:

- Joining Silver orders data with **DimCustomers** and **DimProducts**
- Mapping business keys to surrogate keys
- Creating a fact dataset optimized for analytical workloads
- Performing **upsert operations using Delta Lake MERGE** to support incremental data updates

### Final Data Model

The resulting **Star Schema** consists of:

- **DimCustomers**
- **DimProducts**
- **FactOrders**

This structure enables efficient analytical queries and can be directly consumed by downstream analytics or BI tools.

## End-to-End Pipeline Flow

The following diagram illustrates the complete data pipeline implemented in this project, starting from raw data ingestion to the final analytical data model.

<img width="1555" height="527" alt="image" src="https://github.com/user-attachments/assets/02bcd533-4aac-4896-bcdf-d109679bdaf1" />
# PR test for Mesrai




.
