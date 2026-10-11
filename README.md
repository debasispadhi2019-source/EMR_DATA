# EMR Data Engineering Project

## Overview

This is a multi-phase data engineering project that implements an end-to-end ETL pipeline for Electronic Medical Records (EMR) using Azure cloud services.

## Architecture Diagram

```text
                 EMR Source CSV Files
                    Hospital 1 / 2
                          |
                          v
                Azure Data Lake Storage
                       Gen2
                    Container: raw
                          |
                          v
                Azure Data Factory
                          |
                   Lookup Control Tables
                          |
                          v
                       ForEach
                          |
                          v
                    Copy Activity
                          |
                          v
                 Azure SQL Database
                          |
                 Hospital-specific databases
                          |
                          v
                 EMR Target Tables
```

## Implementation Steps

### Phase 1: ADLS Gen2 to Azure SQL Database

**Status:** ✅ In Progress

This phase implements the metadata-driven data ingestion pipeline that reads EMR CSV files from Azure Data Lake Storage Gen2 and loads them into Azure SQL Database.

**Key Components:**
- Azure Data Lake Storage Gen2 (ADLS) source integration
- Azure Data Factory (ADF) metadata-driven pipeline
- Azure SQL Database with control and target databases
- Lookup, ForEach, and Copy activities
- Dynamic hospital folder and table routing

**For detailed implementation:** See [Phase1_ADLS_TO_SQL_Readme](Phase1_ADLS_TO_SQL_Readme)

### Future Phases

- **Phase 2:** Incremental loading and watermark-based processing
- **Phase 3:** Slowly Changing Dimensions (SCD Type 1 & Type 2)
- **Phase 4:** Audit logging, reconciliation, and error handling
- **Phase 5:** Databricks transformations and analytics

## Repository Structure

```
EMR_DATA/
├── README.md                          (This file - Overview)
├── Phase1_ADLS_TO_SQL_Readme         (Phase 1 detailed documentation)
├── pipeline/                          (ADF pipeline definitions)
├── linkedService/                     (ADF linked services)
├── factory/                           (ADF factory configuration)
├── dataset/                           (ADF datasets)
├── Dataset/                           (Source data files)
└── metadata_config                    (Metadata configuration)
```

## Getting Started

1. Review the [Phase1_ADLS_TO_SQL_Readme](Phase1_ADLS_TO_SQL_Readme) for complete setup instructions
2. Configure Azure resources (Storage, SQL Server, Data Factory)
3. Set up metadata control tables
4. Create linked services and datasets in ADF
5. Deploy and execute the pipeline

---

**For Phase 1 implementation details:** [Phase1_ADLS_TO_SQL_Readme](Phase1_ADLS_TO_SQL_Readme)
