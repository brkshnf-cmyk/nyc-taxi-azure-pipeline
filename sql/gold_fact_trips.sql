-- ============================================================
-- NYC Yellow Taxi — Gold Layer fact_trips & Star Schema
-- File: gold_fact_trips.sql
-- Description: Creates the fact_trips view — the centre of
--              the star schema. One row per completed valid
--              taxi trip. Contains foreign keys to all four
--              dimension tables, raw measures, derived measures
--              and data quality flags carried from silver.
-- Run against: Synapse Serverless SQL Pool — nyc_taxi_db
-- Dependencies: silver.vw_yellow_taxi
--               gold.dim_vendor
--               gold.dim_payment_type
--               gold.dim_rate_code
--               gold.dim_location
--               gold.dim_datetime
-- ============================================================


-- ============================================================
-- KNOWN DATA QUALITY FINDING — total_amount reconciliation
-- ============================================================
-- total_amount does not always equal the sum of individual
-- charge columns. Differences of $3-$5 observed on ~2% of
-- trips, primarily JFK flat-rate and CBD zone trips.
-- Root cause: TLC restructured surcharge columns across
-- 2024-2025 files. Some charges appear in total_amount
-- but not in any individual column in older file versions.
-- Resolution: Use total_amount as the single source of
-- truth for all revenue calculations. Never reconstruct
-- from components.
-- ============================================================


-- ============================================================
-- STEP 1: Create gold Schema
-- Purpose: Separate namespace for business-ready objects.
--          Bronze = raw, Silver = clean, Gold = aggregated.
-- Note: Skip if schema already exists.
-- ============================================================

CREATE SCHEMA gold;


-- ============================================================
-- STEP 2: Create fact_trips View
-- Purpose: Central fact table of the star schema.
--          Grain: one row per completed valid taxi trip.
--          Filters: is_valid_trip = 1 AND is_outlier = 0
--          giving 90,598,245 clean trips across 2024-2025.
--
-- Design decisions:
--   - ROW_NUMBER() surrogate key ordered by pickup datetime
--     and location — stable within a single query execution
--   - pickup_datetime_id truncated to hour to match
--     dim_datetime.datetime_id
--   - Date shortcuts (pickup_date, year, month) on fact
--     table avoid expensive datetime dimension joins for
--     simple date filters in Power BI
--   - tip_pct and avg_speed_mph pre-calculated as derived
--     measures — reduces DAX complexity in Power BI
--   - DECIMAL(18,2) used for all financial columns to
--     prevent arithmetic overflow when summing 90M rows
--   - cbd_congestion_fee included — new column in 2025
--     files for NYC Central Business District congestion
--     pricing. Returns 0 for 2024 files via silver view
--     COALESCE handling.
-- ============================================================

CREATE OR ALTER VIEW gold.fact_trips AS
SELECT
    -- =========================================
    -- SURROGATE KEY
    -- =========================================
    ROW_NUMBER() OVER (
        ORDER BY pickup_datetime, pickup_location_id
    )                                               AS trip_sk,

    -- =========================================
    -- FOREIGN KEYS
    -- =========================================
    vendor_id,
    payment_type_id,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,

    -- Truncated to hour — matches dim_datetime.datetime_id
    DATEADD(HOUR,
        DATEDIFF(HOUR, 0, pickup_datetime), 0)      AS pickup_datetime_id,

    -- =========================================
    -- DEGENERATE DIMENSIONS
    -- =========================================
    store_and_fwd_flag,

    -- =========================================
    -- DATE SHORTCUTS
    -- Avoids datetime dimension join for simple
    -- date filters in Power BI
    -- =========================================
    CAST(pickup_datetime AS DATE)                   AS pickup_date,
    YEAR(pickup_datetime)                           AS pickup_year,
    MONTH(pickup_datetime)                          AS pickup_month,

    -- =========================================
    -- FINANCIAL MEASURES
    -- Use total_amount as definitive revenue figure
    -- See data quality finding note above
    -- =========================================
    CAST(fare_amount AS DECIMAL(18,2))              AS fare_amount,
    CAST(extra AS DECIMAL(18,2))                    AS extra,
    CAST(mta_tax AS DECIMAL(18,2))                  AS mta_tax,
    CAST(tip_amount AS DECIMAL(18,2))               AS tip_amount,
    CAST(tolls_amount AS DECIMAL(18,2))             AS tolls_amount,
    CAST(improvement_surcharge AS DECIMAL(18,2))    AS improvement_surcharge,
    CAST(congestion_surcharge AS DECIMAL(18,2))     AS congestion_surcharge,
    CAST(airport_fee AS DECIMAL(18,2))              AS airport_fee,
    CAST(cbd_congestion_fee AS DECIMAL(18,2))       AS cbd_congestion_fee,
    CAST(total_amount AS DECIMAL(18,2))             AS total_amount,

    -- =========================================
    -- TRIP MEASURES
    -- =========================================
    trip_distance_miles,
    passenger_count,
    trip_duration_mins,

    -- =========================================
    -- DERIVED MEASURES
    -- Pre-calculated to reduce Power BI DAX complexity
    -- =========================================

    -- Tip as percentage of total fare
    CAST(
        CASE WHEN total_amount > 0
             THEN tip_amount / total_amount * 100
             ELSE 0 END
    AS DECIMAL(5,2))                                AS tip_pct,

    -- Average speed in miles per hour
    CAST(
        CASE WHEN trip_duration_mins > 0
             THEN trip_distance_miles / trip_duration_mins * 60
             ELSE 0 END
    AS DECIMAL(5,2))                                AS avg_speed_mph,

    -- =========================================
    -- QUALITY FLAGS — carried from silver
    -- =========================================
    is_valid_trip,
    is_outlier,
    is_passenger_known

FROM silver.vw_yellow_taxi
WHERE is_valid_trip = 1
AND   is_outlier    = 0;


-- ============================================================
-- STEP 3: Verify fact_trips — Row Count
-- Purpose: Confirm row count matches expected valid trip count
-- Expected: ~90,598,245 rows
-- ============================================================

SELECT COUNT(*) AS total_trips
FROM gold.fact_trips;


-- ============================================================
-- STEP 4: Verify fact_trips — Sample Rows
-- Purpose: Confirm all 29 columns present and correct,
--          measures look sensible, no unexpected nulls.
-- ============================================================

SELECT TOP 10 *
FROM gold.fact_trips;


-- ============================================================
-- STEP 5: Verify fact_trips — Measure Ranges
-- Purpose: Confirm financial and trip measures are within
--          expected ranges after quality filtering.
-- Expected: no negative fares, distances 0-100 miles,
--           durations positive, speed 0-80mph typical
-- ============================================================

SELECT
    MIN(fare_amount)            AS min_fare,
    MAX(fare_amount)            AS max_fare,
    AVG(fare_amount)            AS avg_fare,
    MIN(trip_distance_miles)    AS min_distance,
    MAX(trip_distance_miles)    AS max_distance,
    AVG(trip_distance_miles)    AS avg_distance,
    AVG(tip_pct)                AS avg_tip_pct,
    AVG(avg_speed_mph)          AS avg_speed
FROM gold.fact_trips;


-- ============================================================
-- STEP 6: Verify total_amount Reconciliation
-- Purpose: Understand the known data quality finding —
--          difference between sum of components and
--          total_amount. Documents the issue for reference.
-- Finding: $3-$5 difference on ~2% of trips — primarily
--          JFK flat-rate and CBD congestion zone trips.
--          Root cause: TLC surcharge column restructuring
--          across 2024-2025 file versions.
-- ============================================================

SELECT TOP 20
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    CAST(
        fare_amount + extra + mta_tax + tip_amount +
        tolls_amount + improvement_surcharge +
        congestion_surcharge + airport_fee +
        cbd_congestion_fee
    AS DECIMAL(10,2))               AS calculated_total,
    total_amount                    AS actual_total,
    CAST(
        total_amount - (
        fare_amount + extra + mta_tax + tip_amount +
        tolls_amount + improvement_surcharge +
        congestion_surcharge + airport_fee +
        cbd_congestion_fee)
    AS DECIMAL(10,2))               AS difference
FROM gold.fact_trips
ORDER BY difference DESC;


-- ============================================================
-- STEP 7: Verify All Dimension Row Counts
-- Purpose: Quick single-query check all dimensions are
--          intact before running star schema join test.
-- Expected: vendor=3, payment=6, rate=7, location=265
-- ============================================================

SELECT 'dim_vendor'       AS dim_name, COUNT(*) AS row_count FROM gold.dim_vendor
UNION ALL
SELECT 'dim_payment_type', COUNT(*) FROM gold.dim_payment_type
UNION ALL
SELECT 'dim_rate_code',    COUNT(*) FROM gold.dim_rate_code
UNION ALL
SELECT 'dim_location',     COUNT(*) FROM gold.dim_location;


-- ============================================================
-- STEP 8: Full Star Schema Join Test
-- Purpose: Validate all dimensions join correctly to fact
--          table returning meaningful labels for all codes.
--          This is the query pattern Power BI will use.
-- ============================================================

SELECT TOP 100
    v.vendor_name,
    p.payment_description,
    r.rate_description,
    pu.zone_name            AS pickup_zone,
    pu.borough              AS pickup_borough,
    do.zone_name            AS dropoff_zone,
    do.borough              AS dropoff_borough,
    f.pickup_date,
    f.pickup_year,
    f.pickup_month,
    f.fare_amount,
    f.tip_amount,
    f.tip_pct,
    f.total_amount,
    f.trip_distance_miles,
    f.trip_duration_mins,
    f.avg_speed_mph,
    f.passenger_count,
    f.cbd_congestion_fee
FROM gold.fact_trips f
LEFT JOIN gold.dim_vendor       v  ON f.vendor_id           = v.vendor_id
LEFT JOIN gold.dim_payment_type p  ON f.payment_type_id     = p.payment_type_id
LEFT JOIN gold.dim_rate_code    r  ON f.rate_code_id         = r.rate_code_id
LEFT JOIN gold.dim_location     pu ON f.pickup_location_id   = pu.location_id
LEFT JOIN gold.dim_location     do ON f.dropoff_location_id  = do.location_id;


-- ============================================================
-- STEP 9: Borough Revenue Summary
-- Purpose: Business-level aggregation showing total revenue,
--          trip count, avg fare and tip rate by pickup borough.
--          Manhattan expected to dominate by large margin.
-- ============================================================

SELECT
    pu.borough                              AS pickup_borough,
    COUNT(*)                                AS total_trips,
    CAST(SUM(f.total_amount) AS DECIMAL(18,2)) AS total_revenue,
    AVG(f.fare_amount)                      AS avg_fare,
    AVG(f.tip_pct)                          AS avg_tip_pct,
    AVG(f.trip_distance_miles)              AS avg_distance
FROM gold.fact_trips f
LEFT JOIN gold.dim_location pu ON f.pickup_location_id = pu.location_id
GROUP BY pu.borough
ORDER BY total_revenue DESC;