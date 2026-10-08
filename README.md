# AstroLake

A scalable, decoupled lakehouse pipeline designed to ingest, parse, and catalog real-time astronomical transient alerts from the **Zwicky Transient Facility (ZTF)** and the **Vera C. Rubin Observatory's Legacy Survey of Space and Time (LSST)**.

---

## Architecture & Workflow

The ingestion DAG executes a 3-task lifecycle:

1. **Parse & Stage (Avro to Parquet):** Consumes raw Avro OCF alert packets from Kafka, parses, and stages the records as Parquet files in object storage.
2. **Commit to Iceberg:** Ingests the staged Parquet data into Apache Iceberg tables (Silver layer).
3. **Staging Cleanup:** Prunes the intermediate Parquet files from Step 1.

---

## Quick Start
1. Start the infrastructure.
 ```bash
docker compose up -d
```
2. Configure Airflow via the UI:
- Connection: Create kafka_default for Kafka broker credentials.
- Create Variables: ztf_topics, lsst_topics