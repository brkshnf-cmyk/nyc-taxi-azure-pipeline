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
  bronze/yellow-taxi/year=YYYY/month=MM/trip-data/
        ↓
Azure Synapse Analytics (Serverless SQL Pool)
  ├── silver.vw_yellow_taxi  ← 13 cleaning rules
  ├── gold.fact_trips        ← 90.6M clean trips
  ├── gold.dim_*             ← 5 dimension tables
  └── gold.vw_*              ← 4 pre-aggregated KPI views
        ↓
Power BI Dashboard             ← Phase 3
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Azure Data Lake Storage Gen2 | Medallion storage — Bronze/Silver/Gold |
| Azure Data Factory | Orchestration & automated HTTP ingestion |
| Azure Synapse Analytics | SQL transformation & star schema modelling |
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
│   ├── profiling_bronze_analysis.sql
│   ├── silver_vw_yellow_taxi.sql
│   ├── gold_dimension_tables.sql
│   ├── gold_dim_datetime.sql
│   ├── gold_fact_trips.sql
│   ├── gold_kpi_queries.sql
│   ├── data_quality_validation.sql
│   └── query_optimisation.sql
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

## 🔄 Phase 1 — Ingestion Pipeline

### pl_ingest_yellow_taxi_manual
Main ingestion pipeline — one month at a time:
- Pulls monthly Parquet files directly from TLC CDN via HTTP
- Parameterised by year and month
- Idempotency check — skips if month already successfully loaded
- Binary copy — raw files landed unmodified in bronze layer
- Logs every run to pipeline_run_log control table
- Retry policy: 3 retries, 60 second interval, 1 hour timeout

### pl_backfill_yellow_taxi
Bulk backfill pipeline — multiple months at once:
- Accepts array of year/month objects as parameter
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

### Trigger

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

### Control Table — pipeline_run_log

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

## 🗄️ Phase 2 — Star Schema

```
                    dim_datetime
                         |
dim_vendor ──────── fact_trips ──────── dim_location (pickup)
dim_payment_type ───────|─────────────── dim_location (dropoff)
dim_rate_code ──────────|
```

### Dimensions

| Table | Rows | Description |
|-------|------|-------------|
| dim_vendor | 3 | Taxi technology vendors (CMT, VTS) |
| dim_payment_type | 7 | Payment methods inc Flex Fare (type 0) |
| dim_rate_code | 7 | Rate types inc JFK/Newark flat rates |
| dim_location | 265 | NYC taxi zones and boroughs |
| dim_datetime | Hourly | Time attributes, weekend and peak flags |

### fact_trips

| Property | Value |
|----------|-------|
| Grain | One row per completed valid taxi trip |
| Rows | ~90.6 million clean trips |
| Coverage | January 2024 — present |
| Filter | is_valid_trip = 1 AND is_outlier = 0 |
| Measures | fare, tip, total, distance, duration, speed |
| Derived | tip_pct, avg_speed_mph |

### Silver Layer — 13 Cleaning Rules

| Rule | Action |
|------|--------|
| passenger_count IS NULL | Replace with 0, flag is_passenger_known = 0 |
| Airport_fee IS NULL | Replace with 0 |
| cbd_congestion_fee IS NULL | Replace with 0 (2025+ column only) |
| tip_amount < 0 | Replace with 0 |
| trip_distance <= 0 | Flag is_valid_trip = 0 |
| fare_amount <= 0 | Flag is_valid_trip = 0 |
| total_amount <= 0 | Flag is_valid_trip = 0 |
| dropoff <= pickup | Flag is_valid_trip = 0 |
| YEAR(pickup) < 2024 | Flag is_valid_trip = 0 |
| pickup > GETDATE() | Flag is_valid_trip = 0 |
| trip_distance > 100 | Flag is_outlier = 1 |
| fare_amount > 500 | Flag is_outlier = 1 |
| fare_amount < 0 | Flag is_outlier = 1 |

### Power BI KPI Views

| View | Purpose |
|------|---------|
| vw_monthly_kpi | Monthly trips and revenue trends |
| vw_borough_kpi | Borough comparison by month |
| vw_zone_kpi | Zone-level detail by month |
| vw_hourly_pattern | Hourly patterns weekday vs weekend |

---

## 📊 Key Findings — Phase 2

| Metric | Value |
|--------|-------|
| Total trips analysed | 90,598,922 |
| Total revenue | $2.61 billion |
| Data quality rate | 93.39% valid |
| Top borough by trips | Manhattan (87% of all trips) |
| Top zone by trips | Upper East Side South (4.16M) |
| Top zone by revenue | JFK Airport ($328M) |
| Busiest hour | 6pm (6.25M trips) |
| Busiest day | Thursday (14.06M trips) |
| YoY trip growth | +11.3% (2024 to 2025) |
| YoY revenue growth | +12.1% (2024 to 2025) |
| Tip rate trend | 11.32% → 10.06% → 9.09% (declining) |
| Avg NYC taxi speed | ~11 mph (consistent) |

### Known Data Quality Findings

| Finding | Detail |
|---------|--------|
| total_amount reconciliation | $3-5 difference on ~2% of trips due to TLC surcharge restructuring. Use total_amount as definitive revenue figure. |
| cbd_congestion_fee | New column in 2025 files only. COALESCE returns 0 for 2024 files. |
| payment_type_id = 0 | Flex Fare introduced 2024 — added to dim_payment_type. |
| rate_code_id NULL | Flex Fare trips have NULL rate code — COALESCE to 99 (Unknown). |

---

## 🥉 Bronze Layer Structure

```
bronze/
└── yellow-taxi/
    └── year=YYYY/
        └── month=MM/
            └── trip-data/
                └── yellow_tripdata_YYYY-MM.parquet
```

Partitioned by year/month (Hive-style) for partition pruning in
Synapse Analytics. One monthly file per partition matching the
TLC source publishing frequency.

---

## ✅ Project Progress

- [x] Phase 1 — Ingestion pipeline (complete)
- [x] Phase 2 — SQL modelling & star schema (complete)
- [ ] Phase 3 — Power BI dashboard
- [ ] Phase 4 — Testing & monitoring
- [ ] Phase 5 — Portfolio packaging

---

## 🚀 How to Reproduce

1. Clone this repo
2. Create Azure resources: ADLS Gen2, ADF, Azure SQL, Synapse
3. Deploy linked services from `/linked_services` — update credentials
4. Deploy datasets from `/datasets`
5. Deploy pipelines from `/pipelines`
6. Run Azure SQL scripts to create control table and procedures
7. Publish and activate `tr_tumbling_yellow_taxi_daily` trigger
8. Run Synapse SQL scripts in order:
   - `silver_vw_yellow_taxi.sql`
   - `gold_dimension_tables.sql`
   - `gold_dim_datetime.sql`
   - `gold_fact_trips.sql`
9. Validate with `data_quality_validation.sql`
10. Connect Power BI to Synapse serverless SQL endpoint

---

## 💡 Key Design Decisions

| Decision | Reasoning |
|----------|-----------|
| Binary copy at bronze | ZSTD compression not parseable by ADF. Lands files unmodified — correct for bronze layer. |
| Idempotency at month level | Monthly source — checking year+month prevents re-downloading same file daily. |
| Tumbling window trigger | Enables automatic backfill from start date, passes date to pipeline automatically. |
| Separate backfill pipeline | Keeps concerns separated — daily trigger for ongoing, ForEach for bulk historical loads. |
| Serverless SQL pool | Query data lake directly — no provisioned cluster, pay per TB scanned (~$0.015 per full scan). |
| Views not tables | No data duplication — silver and gold read bronze on demand. Schema changes apply instantly. |
| WITH clause in OPENROWSET | Handles schema evolution — cbd_congestion_fee absent in 2024 files, present in 2025. |
| Pre-aggregated KPI views | Power BI reads summary rows not 90M raw rows — faster refresh, lower cost. |
| DECIMAL(18,2) for financials | Prevents arithmetic overflow when summing across 90M+ rows. |
| Control table in Azure SQL | Full pipeline observability without Azure Monitor — status, file size and duration logged per run. |
