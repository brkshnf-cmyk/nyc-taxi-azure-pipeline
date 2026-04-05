-- ============================================================
-- NYC Yellow Taxi — Silver Layer View
-- File: silver_vw_yellow_taxi.sql
-- Description: Cleans and transforms raw bronze Parquet data
--              into a trusted, analysis-ready silver layer view.
--              Applies type casting, null handling and data
--              quality flags. View is dynamic — automatically
--              picks up new monthly files as they land in bronze.
-- Run against: Synapse Serverless SQL Pool — nyc_taxi_db
-- Dependencies: src_bronze_taxi (external data source)
--               bronze/yellow-taxi/year=*/month=*/trip-data/
-- ============================================================


-- ============================================================
-- STEP 1: Create Silver Schema
-- Purpose: Separate namespace for cleaned/transformed objects.
--          Bronze = raw, Silver = clean, Gold = aggregated.
-- Note: Skip if schema already exists.
-- ============================================================

CREATE SCHEMA silver;


-- ============================================================
-- STEP 2: Create External Data Source
-- Purpose: Shortcut pointing to the yellow-taxi folder in
--          bronze ADLS container. Avoids repeating the full
--          storage URL in every query.
-- Note: Skip if src_bronze_taxi already exists.
-- ============================================================

CREATE EXTERNAL DATA SOURCE src_bronze_taxi
WITH (
    LOCATION = 'https://stnyctaxibrook.dfs.core.windows.net/bronze/yellow-taxi'
);


-- ============================================================
-- STEP 3: Create Silver View
-- Purpose: Single dynamic view that reads all Parquet files
--          across all years and months using wildcards.
--          Automatically includes new monthly files as they
--          land in bronze — no view changes needed.
--
-- Cleaning rules applied:
--   1.  passenger_count IS NULL   -> replace with 0
--                                    flag is_passenger_known = 0
--   2.  Airport_fee IS NULL       -> replace with 0
--   3.  tip_amount < 0            -> replace with 0
--   4.  trip_distance <= 0        -> flag is_valid_trip = 0
--   5.  trip_distance > 100       -> flag is_outlier = 1
--   6.  fare_amount <= 0          -> flag is_valid_trip = 0
--   7.  fare_amount > 500         -> flag is_outlier = 1
--   8.  fare_amount < 0           -> flag is_outlier = 1
--   9.  dropoff <= pickup         -> flag is_valid_trip = 0
--   10. total_amount <= 0         -> flag is_valid_trip = 0
--   11. YEAR(pickup) < 2024       -> flag is_valid_trip = 0
--
-- Profiling findings that drove these rules (Jan 2024):
--   Total rows:          2,964,624
--   Null passenger:        140,162  (4.7%)
--   Null airport fee:      140,162  (4.7%)
--   Zero distance:          60,371  (2.0%)
--   Zero/neg fare:          38,341  (1.3%)
--   Dropoff before pickup:     870  (0.03%)
--   Min fare:               -$899   (impossible)
--   Max distance:        312,722mi  (impossible — GPS error)
--   Earliest trip:      2002-12-31  (impossible — clock error)
-- ============================================================

CREATE OR ALTER VIEW silver.vw_yellow_taxi AS
SELECT
    -- =========================================
    -- IDENTIFIERS
    -- =========================================
    CAST(VendorID AS TINYINT)                           AS vendor_id,
    CAST(RatecodeID AS TINYINT)                         AS rate_code_id,
    CAST(payment_type AS TINYINT)                       AS payment_type_id,
    CAST(PULocationID AS SMALLINT)                      AS pickup_location_id,
    CAST(DOLocationID AS SMALLINT)                      AS dropoff_location_id,

    -- =========================================
    -- DATETIME
    -- =========================================
    CAST(tpep_pickup_datetime AS DATETIME2)             AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS DATETIME2)            AS dropoff_datetime,
    DATEDIFF(MINUTE,
        tpep_pickup_datetime,
        tpep_dropoff_datetime)                          AS trip_duration_mins,

    -- =========================================
    -- TRIP DETAILS
    -- =========================================
    CAST(trip_distance AS DECIMAL(10,2))                AS trip_distance_miles,
    COALESCE(CAST(passenger_count AS TINYINT), 0)       AS passenger_count,
    CAST(store_and_fwd_flag AS CHAR(1))                 AS store_and_fwd_flag,

    -- =========================================
    -- FINANCIALS
    -- =========================================
    CAST(fare_amount AS DECIMAL(10,2))                  AS fare_amount,
    CAST(extra AS DECIMAL(10,2))                        AS extra,
    CAST(mta_tax AS DECIMAL(10,2))                      AS mta_tax,
    CAST(CASE WHEN tip_amount < 0 THEN 0
              ELSE tip_amount END
         AS DECIMAL(10,2))                              AS tip_amount,
    CAST(tolls_amount AS DECIMAL(10,2))                 AS tolls_amount,
    CAST(improvement_surcharge AS DECIMAL(10,2))        AS improvement_surcharge,
    CAST(congestion_surcharge AS DECIMAL(10,2))         AS congestion_surcharge,
    COALESCE(CAST(Airport_fee AS DECIMAL(10,2)), 0)     AS airport_fee,
    CAST(total_amount AS DECIMAL(10,2))                 AS total_amount,

    -- =========================================
    -- DATA QUALITY FLAGS
    -- =========================================
    CASE WHEN passenger_count IS NULL
         THEN 0 ELSE 1
         END                                            AS is_passenger_known,

    CASE WHEN trip_distance <= 0
          OR  fare_amount <= 0
          OR  total_amount <= 0
          OR  tpep_dropoff_datetime <= tpep_pickup_datetime
          OR  YEAR(tpep_pickup_datetime) < 2024
         THEN 0 ELSE 1
         END                                            AS is_valid_trip,

    CASE WHEN trip_distance > 100
          OR  fare_amount > 500
          OR  fare_amount < 0
         THEN 1 ELSE 0
         END                                            AS is_outlier

FROM OPENROWSET(
    BULK 'year=*/month=*/trip-data/*.parquet',
    DATA_SOURCE = 'src_bronze_taxi',
    FORMAT = 'PARQUET'
) AS trips;


-- ============================================================
-- STEP 4: Verify Silver View — Sample Rows
-- Purpose: Confirm view returns clean data with all new
--          columns, correct types and no null airport fees.
-- ============================================================

SELECT TOP 100 *
FROM silver.vw_yellow_taxi;


-- ============================================================
-- STEP 5: Verify Silver View — Valid vs Invalid Breakdown
-- Purpose: Understand the split between good, flagged and
--          outlier trips across the full dataset.
-- Findings (full 2024-2025 dataset):
--   Valid, normal trips:    90,598,245  (93.4%)  avg fare $20.03
--   Valid, outlier trips:        4,679  (0.004%) avg fare $646
--   Invalid, normal trips:  2,767,589  (2.9%)   avg fare $24.23
--   Invalid, outlier trips: 3,646,564  (3.8%)   avg fare -$11.75
-- ============================================================

SELECT
    is_valid_trip,
    is_outlier,
    COUNT(*)                    AS trip_count,
    AVG(fare_amount)            AS avg_fare,
    AVG(trip_distance_miles)    AS avg_distance
FROM silver.vw_yellow_taxi
GROUP BY is_valid_trip, is_outlier
ORDER BY is_valid_trip DESC, is_outlier;


-- ============================================================
-- STEP 6: Verify Silver View — Overall Quality Summary
-- Purpose: Single row summary of total trips and each
--          quality flag count across the full dataset.
-- Findings (full 2024-2025 dataset):
--   Total trips:        97,017,077
--   Invalid trips:       6,414,153  (6.6%)
--   Outlier trips:       3,651,243  (3.8%)
--   Unknown passenger:  17,814,501  (18.4%)
-- ============================================================

SELECT
    COUNT(*)                                                        AS total_trips,
    SUM(CASE WHEN is_valid_trip = 0 THEN 1 ELSE 0 END)             AS invalid_trips,
    SUM(CASE WHEN is_outlier = 1 THEN 1 ELSE 0 END)                AS outlier_trips,
    SUM(CASE WHEN is_passenger_known = 0 THEN 1 ELSE 0 END)        AS unknown_passenger
FROM silver.vw_yellow_taxi;


-- ============================================================
-- STEP 7: Verify Cleaning Rules
-- Purpose: Confirm specific cleaning rules were applied
--          correctly — no negative tips, no null airport fees,
--          sensible trip durations for valid trips only.
-- ============================================================

-- Confirm no negative tips remain
SELECT MIN(tip_amount) AS min_tip
FROM silver.vw_yellow_taxi;

-- Confirm no null airport fees remain
SELECT COUNT(*) AS null_airport_fees
FROM silver.vw_yellow_taxi
WHERE airport_fee IS NULL;

-- Confirm trip duration is sensible for valid trips
SELECT
    MIN(trip_duration_mins) AS min_duration,
    MAX(trip_duration_mins) AS max_duration,
    AVG(trip_duration_mins) AS avg_duration
FROM silver.vw_yellow_taxi
WHERE is_valid_trip = 1;


-- ============================================================
-- GOLD LAYER FILTER — use this WHERE clause in all gold
-- queries and Power BI to exclude bad data from analysis
-- ============================================================

-- WHERE is_valid_trip = 1
-- AND   is_outlier = 0