-- ============================================================
-- NYC Yellow Taxi — Query Performance Optimisation
-- File: query_optimisation.sql
-- Description: Optimised views and query patterns for Power BI
--              and ad-hoc analysis. Implements partition
--              elimination, column selection and pre-aggregation
--              to minimise Synapse serverless scan costs.
-- Run against: Synapse Serverless SQL Pool — nyc_taxi_db
-- Dependencies: silver.vw_yellow_taxi
--               gold.fact_trips
--               gold.dim_location
-- ============================================================


-- ============================================================
-- SYNAPSE SERVERLESS COST & OPTIMISATION REFERENCE
-- ============================================================
-- PRICING:  $5 per TB scanned
--           Full dataset ~3GB = ~$0.015 per full scan
--           $10 budget allows ~666 full scans safely
--
-- RULE 1 — PARTITION ELIMINATION
--   Use specific BULK paths for date-filtered queries.
--   Synapse cannot push WHERE clause filters down to the
--   file system when using views — it scans all files first.
--   BAD:  WHERE YEAR(pickup_datetime) = 2024 on silver view
--   GOOD: BULK 'year=2024/month=06/trip-data/*.parquet'
--
-- RULE 2 — COLUMN SELECTION
--   Never use SELECT * in production. Parquet is columnar —
--   each column is stored separately. Selecting 4 columns
--   instead of 20 reads 80% less data — up to 5x cheaper.
--   BAD:  SELECT * FROM silver.vw_yellow_taxi
--   GOOD: SELECT pickup_datetime, fare_amount, tip_amount
--
-- RULE 3 — FILTER EARLY
--   Apply WHERE clause as early as possible.
--   Push filters into OPENROWSET not outer SELECT.
--
-- RULE 4 — PRE-AGGREGATED VIEWS FOR POWER BI
--   Connect Power BI to gold KPI views not raw fact_trips.
--   Pre-aggregated views return summary rows — much faster
--   and cheaper than scanning 90M rows on every refresh.
--
-- RULE 5 — AVOID JOINS ON LARGE TABLES IN SQL
--   Joining 90M fact rows to dimensions in SQL is expensive.
--   Use pre-aggregated views that include dimension labels.
--   Let Power BI relationship model handle dimension joins.
-- ============================================================


-- ============================================================
-- SECTION 1: PARTITION ELIMINATION DEMONSTRATION
-- ============================================================

-- ============================================================
-- BAD QUERY — scans all files then filters in memory
-- Synapse reads every parquet file across all years/months
-- before applying the YEAR/MONTH filter.
-- Data processed: ~3GB (full dataset scan)
-- ============================================================

SELECT COUNT(*) AS total_trips
FROM silver.vw_yellow_taxi
WHERE YEAR(pickup_datetime) = 2024
AND   MONTH(pickup_datetime) = 6;


-- ============================================================
-- GOOD QUERY — partition elimination via specific BULK path
-- Synapse reads only year=2024/month=06 folder.
-- Data processed: ~100MB (one month only)
-- Savings: ~97% less data scanned vs bad query
-- ============================================================

SELECT COUNT(*) AS total_trips
FROM OPENROWSET(
    BULK 'year=2024/month=06/trip-data/*.parquet',
    DATA_SOURCE = 'src_bronze_taxi',
    FORMAT = 'PARQUET'
) WITH (
    tpep_pickup_datetime DATETIME2
) AS trips;


-- ============================================================
-- SECTION 2: COLUMN SELECTION DEMONSTRATION
-- ============================================================

-- ============================================================
-- BAD QUERY — reads all 20 columns unnecessarily
-- ============================================================

SELECT TOP 1000000 *
FROM silver.vw_yellow_taxi
WHERE is_valid_trip = 1;


-- ============================================================
-- GOOD QUERY — reads only 4 needed columns
-- Parquet columnar format means 80% less data scanned
-- ============================================================

SELECT TOP 1000000
    pickup_datetime,
    fare_amount,
    tip_amount,
    total_amount
FROM silver.vw_yellow_taxi
WHERE is_valid_trip = 1;


-- ============================================================
-- SECTION 3: PRE-AGGREGATED GOLD VIEWS FOR POWER BI
-- ============================================================

-- ============================================================
-- VIEW 1: vw_monthly_kpi
-- Purpose: Monthly trip and revenue summary for Power BI
--          time series charts and trend analysis.
--          Power BI reads ~26 summary rows instead of 90M.
-- ============================================================

CREATE OR ALTER VIEW gold.vw_monthly_kpi AS
SELECT
    pickup_year,
    pickup_month,
    DATENAME(MONTH,
        DATEFROMPARTS(pickup_year, pickup_month, 1)) AS month_name,
    COUNT(*)                                        AS total_trips,
    CAST(SUM(CAST(total_amount AS FLOAT))
        AS DECIMAL(20,2))                           AS total_revenue,
    CAST(AVG(fare_amount) AS DECIMAL(10,2))         AS avg_fare,
    CAST(AVG(tip_pct) AS DECIMAL(10,2))             AS avg_tip_pct,
    CAST(AVG(trip_distance_miles) AS DECIMAL(10,2)) AS avg_distance,
    CAST(AVG(CAST(trip_duration_mins AS FLOAT))
        AS DECIMAL(10,2))                           AS avg_duration
FROM gold.fact_trips
GROUP BY pickup_year, pickup_month;


-- ============================================================
-- VIEW 2: vw_borough_kpi
-- Purpose: Borough-level trip and revenue summary broken
--          down by year and month. Drives borough comparison
--          charts and map visuals in Power BI.
-- ============================================================

CREATE OR ALTER VIEW gold.vw_borough_kpi AS
SELECT
    l.borough,
    f.pickup_year,
    f.pickup_month,
    COUNT(*)                                        AS total_trips,
    CAST(SUM(CAST(f.total_amount AS FLOAT))
        AS DECIMAL(20,2))                           AS total_revenue,
    CAST(AVG(f.fare_amount) AS DECIMAL(10,2))       AS avg_fare,
    CAST(AVG(f.tip_pct) AS DECIMAL(10,2))           AS avg_tip_pct
FROM gold.fact_trips f
LEFT JOIN gold.dim_location l ON f.pickup_location_id = l.location_id
GROUP BY l.borough, f.pickup_year, f.pickup_month;


-- ============================================================
-- VIEW 3: vw_zone_kpi
-- Purpose: Zone-level trip and revenue summary broken down
--          by year and month. Drives detailed zone map and
--          top pickup zone visuals in Power BI.
-- ============================================================

CREATE OR ALTER VIEW gold.vw_zone_kpi AS
SELECT
    pu.zone_name                                    AS pickup_zone,
    pu.borough                                      AS pickup_borough,
    f.pickup_year,
    f.pickup_month,
    COUNT(*)                                        AS total_trips,
    CAST(SUM(CAST(f.total_amount AS FLOAT))
        AS DECIMAL(20,2))                           AS total_revenue,
    CAST(AVG(f.fare_amount) AS DECIMAL(10,2))       AS avg_fare,
    CAST(AVG(f.tip_pct) AS DECIMAL(10,2))           AS avg_tip_pct
FROM gold.fact_trips f
LEFT JOIN gold.dim_location pu ON f.pickup_location_id = pu.location_id
GROUP BY pu.zone_name, pu.borough, f.pickup_year, f.pickup_month;


-- ============================================================
-- VIEW 4: vw_hourly_pattern
-- Purpose: Hourly trip pattern split by weekday vs weekend.
--          Drives hour-of-day and peak/off-peak visuals.
--          tip_pct calculated as tip/total_amount * 100
--          renamed avg_tip_pct_of_total for clarity.
-- Note: tip_pct not available on silver view directly —
--       calculated inline using tip_amount/total_amount.
--       Matches fact_trips tip_pct definition exactly.
-- ============================================================

CREATE OR ALTER VIEW gold.vw_hourly_pattern AS
SELECT
    DATEPART(HOUR, pickup_datetime)                 AS hour_num,
    CASE WHEN DATEPART(dw, pickup_datetime) IN (1,7)
         THEN 'Weekend' ELSE 'Weekday'
         END                                        AS day_type,
    COUNT(*)                                        AS total_trips,
    CAST(AVG(fare_amount) AS DECIMAL(10,2))         AS avg_fare,
    CAST(
        AVG(CASE WHEN total_amount > 0
                 THEN tip_amount / total_amount * 100
                 ELSE 0 END)
    AS DECIMAL(10,2))                               AS avg_tip_pct_of_total,
    CAST(AVG(trip_distance_miles) AS DECIMAL(10,2)) AS avg_distance
FROM silver.vw_yellow_taxi
WHERE is_valid_trip = 1
AND   is_outlier    = 0
GROUP BY
    DATEPART(HOUR, pickup_datetime),
    CASE WHEN DATEPART(dw, pickup_datetime) IN (1,7)
         THEN 'Weekend' ELSE 'Weekday'
         END;


-- ============================================================
-- SECTION 4: TEST OPTIMISED VIEWS
-- ============================================================

-- ============================================================
-- TEST 1: Monthly KPI view
-- Expected: 26 rows (Jan 2024 — Feb 2026 + ongoing)
-- ============================================================

SELECT *
FROM gold.vw_monthly_kpi
ORDER BY pickup_year, pickup_month;


-- ============================================================
-- TEST 2: Borough KPI view
-- Expected: boroughs x months rows
-- Manhattan should dominate total_revenue
-- ============================================================

SELECT *
FROM gold.vw_borough_kpi
ORDER BY pickup_year, total_revenue DESC;


-- ============================================================
-- TEST 3: Zone KPI view
-- Expected: zones x months rows
-- JFK Airport should show highest avg_fare
-- ============================================================

SELECT *
FROM gold.vw_zone_kpi
ORDER BY pickup_year, total_revenue DESC;


-- ============================================================
-- TEST 4: Hourly pattern view
-- Expected: 48 rows (24 hours x 2 day types)
-- Hour 18 should be busiest, hour 4 quietest
-- ============================================================

SELECT *
FROM gold.vw_hourly_pattern
ORDER BY hour_num, day_type;


-- ============================================================
-- SECTION 5: COST ESTIMATION
-- ============================================================

-- ============================================================
-- DATA VOLUME BY YEAR
-- Purpose: Understand data distribution for cost planning
--          and to track growth year on year.
-- ============================================================

SELECT
    pickup_year,
    COUNT(*)                                        AS total_trips,
    CAST(SUM(CAST(total_amount AS FLOAT))
        AS DECIMAL(20,2))                           AS total_revenue,
    CAST(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER()
    AS DECIMAL(5,2))                                AS pct_of_total
FROM gold.fact_trips
GROUP BY pickup_year
ORDER BY pickup_year;