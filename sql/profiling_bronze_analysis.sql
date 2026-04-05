-- ============================================================
-- NYC Yellow Taxi — Bronze Layer Data Profiling
-- File: profiling_bronze_analysis.sql
-- Description: Data quality analysis on raw bronze Parquet files
--              Results drive silver layer cleaning rules
-- Run against: Synapse Serverless SQL Pool — nyc_taxi_db
-- ============================================================


-- ============================================================
-- QUERY 1: Row Count
-- Purpose: Understand the volume of data in the bronze layer
--          for January 2024. Run separately for each month
--          by changing the year and month in the BULK path.
-- Finding: 2,964,624 rows in January 2024 alone
-- ============================================================

SELECT COUNT(*) AS row_count
FROM OPENROWSET(
    BULK 'https://stnyctaxibrook.dfs.core.windows.net/bronze/yellow-taxi/year=2024/month=01/trip-data/yellow_tripdata_2024-01.parquet',
    FORMAT = 'PARQUET'
) AS trips;


-- ============================================================
-- QUERY 2: Column Names & Sample Data
-- Purpose: See all 19 columns and their data types by
--          inspecting a small sample of raw rows.
-- Finding: 19 columns including VendorID, pickup/dropoff
--          datetime, fares, distances, location IDs.
--          store_and_fwd_flag is Y/N string — needs casting.
--          Airport_fee is a newer column — may be null in
--          older data files.
-- ============================================================

SELECT TOP 5 *
FROM OPENROWSET(
    BULK 'https://stnyctaxibrook.dfs.core.windows.net/bronze/yellow-taxi/year=2024/month=01/trip-data/yellow_tripdata_2024-01.parquet',
    FORMAT = 'PARQUET'
) AS trips;


-- ============================================================
-- QUERY 3: Null Analysis
-- Purpose: Identify which columns contain nulls and how many.
--          Null counts drive default value decisions in silver.
-- Finding: passenger_count and Airport_fee both have 140,162
--          nulls (4.7% of rows) — same rows, likely a specific
--          vendor or trip type that does not record these fields.
--          All other key columns have zero nulls.
-- Silver rule: replace nulls with 0 for both columns.
--              Flag is_passenger_known = 0 for passenger nulls.
-- ============================================================

SELECT
    COUNT(*)                                                        AS total_rows,
    SUM(CASE WHEN VendorID IS NULL THEN 1 ELSE 0 END)              AS null_vendor,
    SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END)       AS null_passengers,
    SUM(CASE WHEN trip_distance IS NULL THEN 1 ELSE 0 END)         AS null_distance,
    SUM(CASE WHEN PULocationID IS NULL THEN 1 ELSE 0 END)          AS null_pickup_loc,
    SUM(CASE WHEN DOLocationID IS NULL THEN 1 ELSE 0 END)          AS null_dropoff_loc,
    SUM(CASE WHEN fare_amount IS NULL THEN 1 ELSE 0 END)           AS null_fare,
    SUM(CASE WHEN tip_amount IS NULL THEN 1 ELSE 0 END)            AS null_tip,
    SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END)          AS null_total,
    SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END)          AS null_payment,
    SUM(CASE WHEN Airport_fee IS NULL THEN 1 ELSE 0 END)           AS null_airport_fee
FROM OPENROWSET(
    BULK 'https://stnyctaxibrook.dfs.core.windows.net/bronze/yellow-taxi/year=2024/month=01/trip-data/yellow_tripdata_2024-01.parquet',
    FORMAT = 'PARQUET'
) AS trips;


-- ============================================================
-- QUERY 4: Data Quality Flag Analysis
-- Purpose: Count known bad data patterns that need flagging
--          in the silver layer. We never delete bad rows —
--          we flag them so downstream consumers can filter.
-- Findings:
--   zero_distance     = 60,371  (2.0%)  — likely cancelled trips
--   zero_or_neg_fare  = 38,341  (1.3%)  — comp or error trips
--   zero_passengers   = 31,465  (1.1%)  — meter errors
--   negative_tip      = 102     (0.003%)— refunds, set to 0
--   zero_or_neg_total = 35,920  (1.2%)  — comp or error trips
--   dropoff_before_pickup = 870 (0.03%) — data entry errors
-- ============================================================

SELECT
    COUNT(*)                                                            AS total_rows,
    SUM(CASE WHEN trip_distance <= 0 THEN 1 ELSE 0 END)                AS zero_distance,
    SUM(CASE WHEN fare_amount <= 0 THEN 1 ELSE 0 END)                  AS zero_or_neg_fare,
    SUM(CASE WHEN passenger_count <= 0 THEN 1 ELSE 0 END)              AS zero_passengers,
    SUM(CASE WHEN tip_amount < 0 THEN 1 ELSE 0 END)                    AS negative_tip,
    SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END)                 AS zero_or_neg_total,
    SUM(CASE WHEN tpep_dropoff_datetime <= tpep_pickup_datetime
        THEN 1 ELSE 0 END)                                             AS dropoff_before_pickup
FROM OPENROWSET(
    BULK 'https://stnyctaxibrook.dfs.core.windows.net/bronze/yellow-taxi/year=2024/month=01/trip-data/yellow_tripdata_2024-01.parquet',
    FORMAT = 'PARQUET'
) AS trips;


-- ============================================================
-- QUERY 5: Date Range & Fare/Distance Distribution
-- Purpose: Validate date coverage and understand value ranges
--          for key numeric columns. Identifies extreme outliers.
-- Findings:
--   earliest_trip  = 2002-12-31  — impossible, meter clock error
--   latest_trip    = 2024-02-01  — acceptable, late night trips
--   min_fare       = -899        — impossible, data error
--   max_fare       = 5000        — extreme outlier, flag it
--   avg_fare       = $18.18      — looks realistic
--   min_distance   = 0           — known zero distance issue
--   max_distance   = 312,722mi   — impossible, GPS malfunction
--   avg_distance   = 3.65 miles  — looks realistic
-- ============================================================

SELECT
    MIN(tpep_pickup_datetime)   AS earliest_trip,
    MAX(tpep_pickup_datetime)   AS latest_trip,
    MIN(fare_amount)            AS min_fare,
    MAX(fare_amount)            AS max_fare,
    AVG(fare_amount)            AS avg_fare,
    MIN(trip_distance)          AS min_distance,
    MAX(trip_distance)          AS max_distance,
    AVG(trip_distance)          AS avg_distance
FROM OPENROWSET(
    BULK 'https://stnyctaxibrook.dfs.core.windows.net/bronze/yellow-taxi/year=2024/month=01/trip-data/yellow_tripdata_2024-01.parquet',
    FORMAT = 'PARQUET'
) AS trips;


-- ============================================================
-- SILVER LAYER CLEANING RULES — derived from profiling above
-- ============================================================
-- 1.  passenger_count IS NULL   -> replace with 0
--                                  flag is_passenger_known = 0
-- 2.  Airport_fee IS NULL       -> replace with 0
-- 3.  tip_amount < 0            -> replace with 0
-- 4.  trip_distance <= 0        -> flag is_valid_trip = 0
-- 5.  trip_distance > 100       -> flag is_outlier = 1
-- 6.  fare_amount <= 0          -> flag is_valid_trip = 0
-- 7.  fare_amount > 500         -> flag is_outlier = 1
-- 8.  dropoff <= pickup         -> flag is_valid_trip = 0
-- 9.  total_amount <= 0         -> flag is_valid_trip = 0
-- 10. YEAR(pickup) != file year -> flag is_valid_trip = 0
-- ============================================================