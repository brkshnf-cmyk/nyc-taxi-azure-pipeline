# 🚕 NYC Yellow Taxi — Azure Data Engineering Pipeline

## Project Overview
End-to-end Azure data engineering pipeline ingesting NYC TLC Yellow Taxi
trip records into a medallion architecture (Bronze → Silver → Gold),
modelled as a star schema and visualised in Power BI.

Built as a portfolio project demonstrating production-grade data
engineering practices on Microsoft Azure.

---

## 🏗️ Architecture
```
NYC TLC CDN (HTTP) — monthly Parquet files
        ↓
Azure Data Factory
  ├── Lookup → idempotency check (Azure SQL)
  ├── If not loaded → Binary Copy Activity
  └── Stored Procedure → log to control table
        ↓
ADLS Gen2 Bronze Layer
  bronze/yellow-taxi/year=YYYY/month=MM/
        ↓
Azure Synapse Analytics        ← Phase 2
  Silver → Gold (star schema)
        ↓
Power BI Dashboard             ← Phase 3
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Azure Data Lake Storage Gen2 | Medallion storage — Bronze/Silver/Gold |
| Azure Data Factory | Orchestration & automated HTTP ingestion |
| Azure Synapse Analytics | SQL transformation & modelling |
| Azure SQL Database | Pipeline control & logging table |
| Power BI | Dashboard & reporting |
| GitHub | Version control |

---

## 📁 Repository Structure
```
nyc-taxi-azure-pipeline/
├── pipelines/
│   ├── pl_ingest_yellow_taxi_manual.json
│   ├── pl_backfill_yellow_taxi.json
│   └── triggers/
│       └── tr_tumbling_yellow_taxi_daily.json
├── linked_services/
│   ├── ls_adls_nyctaxi.json
│   ├── ls_http_tlc.json
│   └── ls_azure_sql_control.json
├── datasets/
│   ├── ds_http_yellow_taxi_source.json
│   ├── ds_sink_yellow_taxi_bronze.json
│   └── ds_azure_sql_control.json
├── sql/
│   ├── create_pipeline_run_log.sql
│   ├── usp_log_pipeline_start.sql
│   ├── usp_log_pipeline_end.sql
│   └── usp_check_already_loaded.sql
└── docs/                        ← architecture diagrams (Phase 5)
```

---

## 📦 Dataset

| Field | Value |
|-------|-------|
| Provider | NYC Taxi & Limousine Commission (TLC) |
| Dataset | Yellow Taxi Trip Records |
| Format | Parquet (ZSTD compressed) |
| Source | https://d37ci6vzurychx.cloudfront.net/trip-data/ |
| Coverage | January 2024 — present |
| Update frequency | Monthly |

---

## 🔄 Pipelines

### pl_ingest_yellow_taxi_manual
Main ingestion pipeline — designed for one month at a time:
- Pulls monthly Parquet files directly from TLC CDN via HTTP
- Parameterised by year and month
- Idempotency check — skips if month already successfully loaded
- Binary copy — raw files landed unmodified in bronze layer
- Logs every run to pipeline_run_log control table
- Retry policy: 3 retries, 60 second interval, 1 hour timeout

### pl_backfill_yellow_taxi
Bulk backfill pipeline — designed for loading multiple months:
- Accepts an array of year/month objects as a parameter
- ForEach loop processes up to 4 months in parallel
- Calls pl_ingest_yellow_taxi_manual for each month
- Safe to rerun — idempotency prevents duplicate loads

Example parameter:
```json
[
  {"year":"2024","month":"11"},
  {"year":"2024","month":"12"}
]
```

---

## ⚙️ Trigger

| Field | Value |
|-------|-------|
| Name | tr_tumbling_yellow_taxi_daily |
| Type | Tumbling window |
| Schedule | Daily at 06:00 UTC |
| Start date | 2024-01-01 |
| Concurrency | 5 windows in parallel |

**Design note:** The TLC source publishes monthly files so a daily
trigger is intentionally over-engineered here. Days 2-31 of each
month are skipped cleanly by the idempotency check. This pattern
demonstrates that the pipeline is safe to run at any frequency
without risk of duplicate loads. In production, a storage event
trigger firing when a new file lands would be the optimal approach
for a monthly source.

The tumbling window start date of 2024-01-01 caused ADF to
automatically backfill all windows from that date to present —
loading 2 full years of historical data without manual intervention.

---

## 🗃️ Control Table — pipeline_run_log

Every pipeline run is logged to Azure SQL:

| Column | Description |
|--------|-------------|
| log_id | Auto-increment primary key |
| pipeline_name | Name of the ADF pipeline |
| run_id | Unique ADF run ID |
| p_year / p_month / p_day | Partition parameters |
| status | RUNNING / SUCCESS / FAILED |
| files_written | Number of files copied |
| bytes_written | Size of data transferred |
| start_time / end_time | Run duration |
| error_message | Error detail if failed |

---

## 🥉 Bronze Layer Structure
```
bronze/
└── yellow-taxi/
    └── year=YYYY/
        └── month=MM/
            └── yellow_tripdata_YYYY-MM.parquet
```

Partitioned by year/month (Hive-style) for partition pruning in
Synapse Analytics. One monthly file per partition matching the
TLC source publishing frequency.

---

## ✅ Project Progress

- [x] Phase 1 — Ingestion pipeline (complete)
- [ ] Phase 2 — SQL modelling & star schema
- [ ] Phase 3 — Power BI dashboard
- [ ] Phase 4 — Testing & monitoring
- [ ] Phase 5 — Portfolio packaging

---

## 🚀 How to Reproduce

1. Clone this repo
2. Create Azure resources: ADLS Gen2, ADF, Azure SQL, Synapse
3. Deploy linked services from `/linked_services`
   - Update credentials for your own accounts
4. Deploy datasets from `/datasets`
5. Deploy pipelines from `/pipelines`
6. Run SQL scripts in `/sql` to create control table and procedures
7. Publish and activate `tr_tumbling_yellow_taxi_daily` trigger
8. Monitor runs in ADF Studio → Monitor → Trigger runs

---

## 💡 Key Design Decisions

| Decision | Reasoning |
|----------|-----------|
| Binary copy at bronze layer | TLC files use ZSTD compression which ADF cannot parse natively. Binary copy lands files unmodified — correct for bronze layer. |
| Idempotency at month level | Source publishes monthly — checking year+month prevents re-downloading the same file on every daily trigger run |
| Tumbling window over schedule trigger | Enables automatic backfill of historical data from start date and passes window date to pipeline automatically |
| Separate backfill pipeline | Keeps concerns separated — daily trigger for ongoing loads, ForEach pipeline for bulk historical loads |
| Control table in Azure SQL | Full pipeline observability without needing Azure Monitor — every run logged with status, file size and duration |