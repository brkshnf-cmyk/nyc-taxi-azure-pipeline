-- ============================================================
-- NYC Yellow Taxi — Data Quality Validation Suite
-- File: data_quality_validation.sql
-- Description: Formal validation queries proving pipeline
--              correctness end to end. Run after any schema
--              or pipeline change to confirm nothing broke.
--              Acts as an acceptance test suite for the project.
-- Run against: Synapse Serverless SQL Pool — nyc_taxi_db
--              Pipeline queries run against Azure SQL DB
-- Dependencies: silver.vw_yellow_taxi
--               gold.fact_trips + all dimensions
--               pipeline_run_log (Azure SQL)
-- ============================================================


-- ============================================================
-- ISSUES FOUND & RESOLVED DURING VALIDATION
-- ============================================================
-- 1. payment_type_id = 0 orphans (14.4M trips)
--    Root cause: TLC introduced Flex Fare payment type in 2024
--    Resolution: Added payment_type_id=0 to dim_payment_type
--
-- 2. rate_code_id NULL orphans (14.4M trips — same trips)
--    Root cause: Flex Fare trips have NULL rate_code_id
--    Resolution: COALESCE(rate_code_id, 99) in fact_trips view
--
-- 3. Future dates slipping through (2026-03, 2026-06)
--    Root cause: Meter clock errors in source data
--    Resolution: Added tpep_pickup_datetime > GETDATE() filter
--
-- 4. total_amount reconciliation difference ($3-$5 on ~2%)
--    Root cause: TLC restructured surcharge columns 2024-2025
--    Resolution: Documented as known DQ finding — use
--                total_amount as single source of truth
-- ============================================================


-- ============================================================
-- SECTION 1: BRONZE LAYER VALIDATION
-- ============================================================

-- ============================================================
-- BRONZE VALIDATION 1: File count and row count per month
-- Purpose: Confirm all expected months are present in bronze
--          and each month has a realistic row count.
-- Expected: 26 rows (Jan 2024 — Feb 2026 + ongoing)
--           Each month: 2.5M — 4.5M rows
-- Findings: All months present, row counts realistic
--           Highest: 2025-05 (4,591,845 rows)
--           Lowest:  2024-01 (2,964,624 rows)
-- Note: Edit year=2024 to year=2025 or year=2026 as needed
-- ============================================================

SELECT
    r.filepath(1)               AS year,
    r.filepath(2)               AS month,
    COUNT(*)                    AS row_count
FROM OPENROWSET(
    BULK 'year=2024/month=*/trip-data/*.parquet',
    DATA_SOURCE = 'src_bronze_taxi',
    FORMAT = 'PARQUET'
) AS r
GROUP BY r.filepath(1), r.filepath(2)
ORDER BY r.filepath(1), r.filepath(2);


-- ============================================================
-- SECTION 2: SILVER LAYER VALIDATION
-- ============================================================

-- ============================================================
-- SILVER VALIDATION 1: No nulls in critical columns
-- Purpose: Confirm all key columns are populated after
--          cleaning. Nulls in these columns would break
--          joins and aggregations in the gold layer.
-- Expected: All counts = 0
-- Findings: All 0 — silver cleaning rules working correctly
-- ============================================================

SELECT
    SUM(CASE WHEN vendor_id IS NULL THEN 1 ELSE 0 END)          AS null_vendor,
    SUM(CASE WHEN pickup_datetime IS NULL THEN 1 ELSE 0 END)     AS null_pickup,
    SUM(CASE WHEN dropoff_datetime IS NULL THEN 1 ELSE 0 END)    AS null_dropoff,
    SUM(CASE WHEN pickup_location_id IS NULL THEN 1 ELSE 0 END)  AS null_pickup_loc,
    SUM(CASE WHEN dropoff_location_id IS NULL THEN 1 ELSE 0 END) AS null_dropoff_loc,
    SUM(CASE WHEN fare_amount IS NULL THEN 1 ELSE 0 END)         AS null_fare,
    SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END)        AS null_total
FROM silver.vw_yellow_taxi;


-- ============================================================
-- SILVER VALIDATION 2: No negative tips after cleaning
-- Purpose: Confirm tip_amount < 0 cleaning rule applied.
--          Negative tips are data entry errors — cleaned
--          to 0 in silver view.
-- Expected: min_tip >= 0.00
-- Findings: min_tip = 0.00 ✅  max_tip = 999.99
-- ============================================================

SELECT
    MIN(tip_amount)     AS min_tip,
    MAX(tip_amount)     AS max_tip,
    AVG(tip_amount)     AS avg_tip
FROM silver.vw_yellow_taxi
WHERE is_valid_trip = 1;


-- ============================================================
-- SILVER VALIDATION 3: No future dates after cleaning
-- Purpose: Confirm tpep_pickup_datetime > GETDATE() filter
--          is working. Future dates are meter clock errors.
-- Expected: future_dates = 0
-- Findings: 0 ✅ (before fix: 5 rows with 2026-03, 2026-06)
-- ============================================================

SELECT COUNT(*) AS future_dates
FROM silver.vw_yellow_taxi
WHERE pickup_datetime > GETDATE()
AND   is_valid_trip = 1;


-- ============================================================
-- SILVER VALIDATION 4: No impossible pre-2024 dates
-- Purpose: Confirm YEAR(pickup) < 2024 filter working.
--          Pre-2024 dates are clock errors — earliest seen
--          was 2002-12-31.
-- Expected: pre_2024_dates = 0
-- ============================================================

SELECT COUNT(*) AS pre_2024_dates
FROM silver.vw_yellow_taxi
WHERE YEAR(pickup_datetime) < 2024
AND   is_valid_trip = 1;


-- ============================================================
-- SILVER VALIDATION 5: Quality flag distribution
-- Purpose: Understand the overall data quality of the
--          full bronze dataset before gold filtering.
-- Expected: ~93% valid, ~7% invalid, ~4% outlier
-- Findings:
--   Total rows:    97,017,077
--   Valid trips:   90,602,922  (93.39%)
--   Invalid trips:  6,414,155  (6.61%)
--   Outlier trips:  3,651,243  (3.76%)
-- ============================================================

SELECT
    COUNT(*)                                                    AS total_rows,
    SUM(CASE WHEN is_valid_trip = 1 THEN 1 ELSE 0 END)         AS valid_trips,
    SUM(CASE WHEN is_valid_trip = 0 THEN 1 ELSE 0 END)         AS invalid_trips,
    SUM(CASE WHEN is_outlier = 1 THEN 1 ELSE 0 END)            AS outlier_trips,
    CAST(SUM(CASE WHEN is_valid_trip = 1 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*) AS DECIMAL(5,2))                    AS valid_pct,
    CAST(SUM(CASE WHEN is_valid_trip = 0 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*) AS DECIMAL(5,2))                    AS invalid_pct,
    CAST(SUM(CASE WHEN is_outlier = 1 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*) AS DECIMAL(5,2))                    AS outlier_pct
FROM silver.vw_yellow_taxi;


-- ============================================================
-- SECTION 3: GOLD LAYER VALIDATION
-- ============================================================

-- ============================================================
-- GOLD VALIDATION 1: Dimension referential integrity
-- Purpose: Confirm no orphan records in fact table —
--          every foreign key joins to a dimension row.
--          Orphans cause blank labels in Power BI reports.
-- Expected: All counts = 0
-- Findings (after fixes):
--   Before: orphan_payment=14.4M (payment_type=0 missing)
--           orphan_rate=14.4M (NULL rate_code_id)
--   After:  All 0 ✅
-- Fixes applied:
--   Added payment_type_id=0 (Flex Fare) to dim_payment_type
--   Added COALESCE(rate_code_id, 99) in fact_trips view
-- ============================================================

SELECT
    SUM(CASE WHEN v.vendor_id IS NULL THEN 1 ELSE 0 END)        AS orphan_vendor,
    SUM(CASE WHEN p.payment_type_id IS NULL THEN 1 ELSE 0 END)  AS orphan_payment,
    SUM(CASE WHEN r.rate_code_id IS NULL THEN 1 ELSE 0 END)     AS orphan_rate,
    SUM(CASE WHEN pu.location_id IS NULL THEN 1 ELSE 0 END)     AS orphan_pickup_loc,
    SUM(CASE WHEN do.location_id IS NULL THEN 1 ELSE 0 END)     AS orphan_dropoff_loc
FROM gold.fact_trips f
LEFT JOIN gold.dim_vendor       v  ON f.vendor_id           = v.vendor_id
LEFT JOIN gold.dim_payment_type p  ON f.payment_type_id     = p.payment_type_id
LEFT JOIN gold.dim_rate_code    r  ON f.rate_code_id         = r.rate_code_id
LEFT JOIN gold.dim_location     pu ON f.pickup_location_id   = pu.location_id
LEFT JOIN gold.dim_location     do ON f.dropoff_location_id  = do.location_id;


-- ============================================================
-- GOLD VALIDATION 2: Fact table measure ranges
-- Purpose: Confirm all measures within expected ranges
--          after quality filtering in fact_trips view.
-- Expected: no negatives, sensible maximums
-- Findings:
--   fare:     $0.01 — $500.00  ✅
--   tip:      $0.00 — $999.99  (high tip is valid)
--   total:    $0.01 — $5297.87 (extreme but possible)
--   distance: 0.01  — 99.86mi  ✅
--   duration: 0     — 14881min (extreme — long waits)
--   tip_pct:  0.00  — 535.33%  (tip > fare on some trips)
-- ============================================================

SELECT
    MIN(fare_amount)            AS min_fare,
    MAX(fare_amount)            AS max_fare,
    MIN(tip_amount)             AS min_tip,
    MAX(tip_amount)             AS max_tip,
    MIN(total_amount)           AS min_total,
    MAX(total_amount)           AS max_total,
    MIN(trip_distance_miles)    AS min_distance,
    MAX(trip_distance_miles)    AS max_distance,
    MIN(trip_duration_mins)     AS min_duration,
    MAX(trip_duration_mins)     AS max_duration,
    MIN(tip_pct)                AS min_tip_pct,
    MAX(tip_pct)                AS max_tip_pct
FROM gold.fact_trips;


-- ============================================================
-- GOLD VALIDATION 3: Month continuity check
-- Purpose: Confirm no months are missing from the dataset.
--          A missing month would indicate a pipeline failure
--          or ingestion gap.
-- Expected: 2024=12 months, 2025=12 months, 2026=partial
-- Findings:
--   2024: 12 months present (Jan-Dec) — 39,702,467 trips
--   2025: 12 months present (Jan-Dec) — 44,168,945 trips
--   2026: 3 months present  (Jan-Mar) — 6,726,831 trips
-- ============================================================

SELECT
    pickup_year,
    COUNT(DISTINCT pickup_month)    AS months_present,
    MIN(pickup_month)               AS first_month,
    MAX(pickup_month)               AS last_month,
    SUM(COUNT(*)) OVER
        (PARTITION BY pickup_year)  AS year_total_trips
FROM gold.fact_trips
GROUP BY pickup_year
ORDER BY pickup_year;


-- ============================================================
-- GOLD VALIDATION 4: Duplicate trip check
-- Purpose: Identify potential duplicate trips in fact table.
-- Note: Uses pickup_date (not pickup_datetime) as fact_trips
--       is a view without direct datetime access. High counts
--       at date level are expected — many trips share the same
--       date, zone, fare and duration legitimately.
--       True duplicate check requires pickup_datetime from
--       silver.vw_yellow_taxi for deeper investigation.
-- ============================================================

SELECT COUNT(*) AS duplicate_count
FROM (
    SELECT
        pickup_date,
        pickup_location_id,
        dropoff_location_id,
        fare_amount,
        trip_duration_mins,
        COUNT(*) AS cnt
    FROM gold.fact_trips
    GROUP BY
        pickup_date,
        pickup_location_id,
        dropoff_location_id,
        fare_amount,
        trip_duration_mins
    HAVING COUNT(*) > 1
) AS dupes;


-- ============================================================
-- GOLD VALIDATION 5: Revenue reconciliation
-- Purpose: Confirm gold layer total revenue exactly matches
--          silver layer filtered total. Difference of $0.00
--          proves no data was lost or added during the
--          silver -> gold transformation.
-- Expected: difference = 0.00
-- Findings: difference = $0.00 ✅
--   Silver total = Gold total = $2,612,880,324.77
-- ============================================================

SELECT
    CAST((SELECT SUM(CAST(total_amount AS FLOAT))
          FROM silver.vw_yellow_taxi
          WHERE is_valid_trip = 1
          AND   is_outlier    = 0)
    AS DECIMAL(20,2))                               AS silver_total,
    CAST((SELECT SUM(CAST(total_amount AS FLOAT))
          FROM gold.fact_trips)
    AS DECIMAL(20,2))                               AS gold_total,
    CAST(
        (SELECT SUM(CAST(total_amount AS FLOAT))
         FROM silver.vw_yellow_taxi
         WHERE is_valid_trip = 1 AND is_outlier = 0)
        -
        (SELECT SUM(CAST(total_amount AS FLOAT))
         FROM gold.fact_trips)
    AS DECIMAL(20,2))                               AS difference;


-- ============================================================
-- SECTION 4: PIPELINE CONTROL TABLE VALIDATION
-- Run these against Azure SQL Database — sqldb-nyc-taxi-control
-- ============================================================

-- ============================================================
-- PIPELINE VALIDATION 1: All months successfully loaded
-- Purpose: Confirm control table shows SUCCESS for all
--          expected months — no gaps in ingestion history.
-- Expected: One SUCCESS row per month loaded
-- ============================================================

SELECT
    p_year,
    p_month,
    status,
    files_written,
    bytes_written / 1048576.0           AS mb_written,
    DATEDIFF(SECOND, start_time, end_time) AS duration_secs
FROM pipeline_run_log
WHERE status = 'SUCCESS'
ORDER BY p_year, p_month;


-- ============================================================
-- PIPELINE VALIDATION 2: Failed runs check
-- Purpose: Identify any pipeline runs that failed — should
--          be investigated and rerun if found.
-- Expected: 0 rows
-- ============================================================

SELECT
    pipeline_name,
    p_year,
    p_month,
    status,
    error_message,
    start_time
FROM pipeline_run_log
WHERE status = 'FAILED'
ORDER BY start_time DESC;


-- ============================================================
-- PIPELINE VALIDATION 3: Ingestion performance benchmarks
-- Purpose: Understand pipeline performance baselines.
--          Useful for detecting slowdowns over time.
-- ============================================================

SELECT
    AVG(files_written)                              AS avg_files,
    AVG(bytes_written / 1048576.0)                  AS avg_mb,
    AVG(DATEDIFF(SECOND, start_time, end_time))     AS avg_duration_secs,
    MAX(DATEDIFF(SECOND, start_time, end_time))     AS max_duration_secs,
    MIN(DATEDIFF(SECOND, start_time, end_time))     AS min_duration_secs
FROM pipeline_run_log
WHERE status = 'SUCCESS';


-- ============================================================
-- VALIDATION SUMMARY
-- ============================================================
-- Bronze:  All 26 months present, row counts realistic      ✅
-- Silver:  Zero nulls in critical columns                   ✅
--          Zero negative tips after cleaning                ✅
--          Zero future dates after cleaning                 ✅
--          93.39% valid trip rate                           ✅
-- Gold:    Zero orphans across all 5 dimensions             ✅
--          Revenue reconciliation difference = $0.00        ✅
--          All 24 months present for 2024-2025              ✅
--          Measure ranges sensible                          ✅
-- Pipeline: Control table logs all runs with metrics        ✅
-- ============================================================