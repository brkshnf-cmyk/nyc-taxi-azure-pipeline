-- ============================================================
-- NYC Yellow Taxi — Gold Layer KPI Queries
-- File: gold_kpi_queries.sql
-- Description: Business-level aggregation queries that feed
--              Power BI dashboards. All queries use the gold
--              layer star schema — fact_trips joined to
--              dimension tables. Results represent clean,
--              validated trips only (is_valid_trip=1,
--              is_outlier=0).
-- Run against: Synapse Serverless SQL Pool — nyc_taxi_db
-- Dependencies: gold.fact_trips
--               gold.dim_vendor
--               gold.dim_payment_type
--               gold.dim_rate_code
--               gold.dim_location
-- Cost note:   Each full scan ~$0.015 (1.5 cents)
--              Always use pickup_year/month filters where
--              possible to reduce data scanned
-- ============================================================


-- ============================================================
-- QUERY 1: Revenue & Trip KPIs by Month
-- Purpose: Primary time series metric — trips and revenue
--          per month across the full dataset. Used for
--          trend analysis and seasonality in Power BI.
--
-- Findings:
--   2024: Jan lowest (2.87M trips), Oct highest (3.68M)
--   2025: Jan lowest (3.25M trips), Dec highest (4.05M)
--   2026: Jan-Feb only (partial year — pipeline ongoing)
--   Dec 2025 highest revenue month — $127M, avg fare $22.51
--   Clear seasonality: summer dip, autumn/winter peak
-- ============================================================

SELECT
    f.pickup_year,
    f.pickup_month,
    DATENAME(MONTH,
        DATEFROMPARTS(f.pickup_year, f.pickup_month, 1)) AS month_name,
    COUNT(*)                                        AS total_trips,
    CAST(SUM(CAST(f.total_amount AS FLOAT))
        AS DECIMAL(20,2))                           AS total_revenue,
    CAST(AVG(f.fare_amount) AS DECIMAL(10,2))       AS avg_fare,
    CAST(AVG(f.tip_pct) AS DECIMAL(10,2))           AS avg_tip_pct,
    CAST(AVG(f.trip_distance_miles) AS DECIMAL(10,2)) AS avg_distance,
    CAST(AVG(CAST(f.trip_duration_mins AS FLOAT))
        AS DECIMAL(10,2))                           AS avg_duration
FROM gold.fact_trips f
GROUP BY f.pickup_year, f.pickup_month
ORDER BY f.pickup_year, f.pickup_month;


-- ============================================================
-- QUERY 2: Borough Performance
-- Purpose: Compare trip volume, revenue, fares and tip rates
--          across NYC boroughs. Drives borough heatmap in
--          Power BI.
--
-- Findings:
--   Manhattan:    79.1M trips, $1.93B revenue, $16.37 avg fare
--   Queens:        8.4M trips, $587M revenue, $51.47 avg fare
--   Brooklyn:      2.3M trips, $72.7M revenue, $27.73 avg fare
--   Bronx:         521K trips, $17.4M revenue, $30.05 avg fare
--   EWR:           2.8K trips, $274K revenue, $82.19 avg fare
--   Staten Island: 6.0K trips, $270K revenue, $33.76 avg fare
--
--   Key insight: Manhattan has 87% of trips but lowest avg
--   fare ($16) — short city hops. Queens has 8x higher avg
--   fare — mostly airport runs. EWR highest fare — Newark
--   Airport flat rate surcharge.
-- ============================================================

SELECT
    pu.borough                                      AS pickup_borough,
    COUNT(*)                                        AS total_trips,
    CAST(SUM(CAST(f.total_amount AS FLOAT))
        AS DECIMAL(20,2))                           AS total_revenue,
    CAST(AVG(f.fare_amount) AS DECIMAL(10,2))       AS avg_fare,
    CAST(AVG(f.tip_pct) AS DECIMAL(10,2))           AS avg_tip_pct,
    CAST(AVG(f.trip_distance_miles) AS DECIMAL(10,2)) AS avg_distance
FROM gold.fact_trips f
LEFT JOIN gold.dim_location pu ON f.pickup_location_id = pu.location_id
GROUP BY pu.borough
ORDER BY total_revenue DESC;


-- ============================================================
-- QUERY 3: Top 10 Busiest Pickup Zones
-- Purpose: Identify the highest volume pickup locations.
--          Drives zone-level map in Power BI.
--
-- Findings:
--   1. Upper East Side South — 4.16M trips (Manhattan)
--   2. Midtown Center        — 4.08M trips (Manhattan)
--   3. JFK Airport           — 4.01M trips BUT #1 revenue $328M
--   4. Upper East Side North — 3.72M trips (Manhattan)
--   5. Midtown East          — 2.99M trips (Manhattan)
--   LaGuardia — #9 in trips but $182M revenue
--   Key insight: airports dominate revenue despite lower
--   trip volume due to flat rate fares ($70 JFK, $80+ EWR)
-- ============================================================

SELECT TOP 10
    pu.zone_name                                    AS pickup_zone,
    pu.borough                                      AS borough,
    COUNT(*)                                        AS total_trips,
    CAST(SUM(CAST(f.total_amount AS FLOAT))
        AS DECIMAL(20,2))                           AS total_revenue,
    CAST(AVG(f.fare_amount) AS DECIMAL(10,2))       AS avg_fare
FROM gold.fact_trips f
LEFT JOIN gold.dim_location pu ON f.pickup_location_id = pu.location_id
GROUP BY pu.zone_name, pu.borough
ORDER BY total_trips DESC;


-- ============================================================
-- QUERY 4: Top 10 Most Profitable Routes
-- Purpose: Identify the highest avg revenue routes.
--          Useful for understanding where the money is made.
-- ============================================================

SELECT TOP 10
    pu.zone_name                                    AS pickup_zone,
    do.zone_name                                    AS dropoff_zone,
    pu.borough                                      AS pickup_borough,
    do.borough                                      AS dropoff_borough,
    COUNT(*)                                        AS total_trips,
    CAST(AVG(CAST(f.total_amount AS FLOAT))
        AS DECIMAL(10,2))                           AS avg_total,
    CAST(AVG(f.fare_amount) AS DECIMAL(10,2))       AS avg_fare,
    CAST(AVG(f.tip_pct) AS DECIMAL(10,2))           AS avg_tip_pct,
    CAST(AVG(f.trip_distance_miles) AS DECIMAL(10,2)) AS avg_distance
FROM gold.fact_trips f
LEFT JOIN gold.dim_location pu ON f.pickup_location_id  = pu.location_id
LEFT JOIN gold.dim_location do ON f.dropoff_location_id = do.location_id
GROUP BY pu.zone_name, do.zone_name, pu.borough, do.borough
ORDER BY avg_total DESC;


-- ============================================================
-- QUERY 5: Payment Type Split
-- Purpose: Understand how passengers pay and the impact
--          on tip behaviour. Credit card vs cash split
--          is a key metric for the dashboard.
--
-- Findings:
--   Credit Card: 64.7M trips (71.4%), avg tip $4.32 (14.42%)
--   Unknown/Null:14.5M trips (16.0%), avg tip $0.50 (1.47%)
--   Cash:        10.1M trips (11.2%), avg tip $0.00 (0.00%)
--   Dispute:     964K trips  (1.1%),  avg tip $0.01 (0.02%)
--
--   Note: Blank payment description (15.96%) represents trips
--   where payment_type_id has no match in dim_payment_type.
--   Likely newer vendor codes not in TLC data dictionary.
--   Cash trips show $0 tip — cash tips not recorded in system.
-- ============================================================

SELECT
    p.payment_description,
    COUNT(*)                                        AS total_trips,
    CAST(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER()
    AS DECIMAL(5,2))                                AS trip_pct,
    CAST(SUM(CAST(f.total_amount AS FLOAT))
        AS DECIMAL(20,2))                           AS total_revenue,
    CAST(AVG(f.tip_amount) AS DECIMAL(10,2))        AS avg_tip,
    CAST(AVG(f.tip_pct) AS DECIMAL(10,2))           AS avg_tip_pct
FROM gold.fact_trips f
LEFT JOIN gold.dim_payment_type p ON f.payment_type_id = p.payment_type_id
GROUP BY p.payment_description
ORDER BY total_trips DESC;


-- ============================================================
-- QUERY 6: Vendor Performance Comparison
-- Purpose: Compare Creative Mobile Technologies vs VeriFone
--          across volume, revenue, fares and speed.
-- ============================================================

SELECT
    v.vendor_name,
    COUNT(*)                                        AS total_trips,
    CAST(SUM(CAST(f.total_amount AS FLOAT))
        AS DECIMAL(20,2))                           AS total_revenue,
    CAST(AVG(f.fare_amount) AS DECIMAL(10,2))       AS avg_fare,
    CAST(AVG(f.tip_pct) AS DECIMAL(10,2))           AS avg_tip_pct,
    CAST(AVG(f.trip_distance_miles) AS DECIMAL(10,2)) AS avg_distance,
    CAST(AVG(CAST(f.avg_speed_mph AS FLOAT))
        AS DECIMAL(10,2))                           AS avg_speed
FROM gold.fact_trips f
LEFT JOIN gold.dim_vendor v ON f.vendor_id = v.vendor_id
GROUP BY v.vendor_name
ORDER BY total_trips DESC;


-- ============================================================
-- QUERY 7: Airport vs Non-Airport Trips
-- Purpose: Compare airport runs (JFK/Newark flat rate)
--          vs standard metered trips across all key metrics.
-- ============================================================

SELECT
    CASE WHEN r.rate_description IN ('JFK', 'Newark')
         THEN 'Airport'
         ELSE 'Non-Airport'
         END                                        AS trip_type,
    COUNT(*)                                        AS total_trips,
    CAST(SUM(CAST(f.total_amount AS FLOAT))
        AS DECIMAL(20,2))                           AS total_revenue,
    CAST(AVG(f.fare_amount) AS DECIMAL(10,2))       AS avg_fare,
    CAST(AVG(f.tip_pct) AS DECIMAL(10,2))           AS avg_tip_pct,
    CAST(AVG(f.trip_distance_miles) AS DECIMAL(10,2)) AS avg_distance,
    CAST(AVG(CAST(f.trip_duration_mins AS FLOAT))
        AS DECIMAL(10,2))                           AS avg_duration
FROM gold.fact_trips f
LEFT JOIN gold.dim_rate_code r ON f.rate_code_id = r.rate_code_id
GROUP BY
    CASE WHEN r.rate_description IN ('JFK', 'Newark')
         THEN 'Airport'
         ELSE 'Non-Airport'
         END
ORDER BY total_trips DESC;


-- ============================================================
-- QUERY 8: Year over Year Comparison
-- Purpose: Top-level year on year performance summary.
--          Headline metric for portfolio and interviews.
--
-- Findings:
--   2024: 39.7M trips, $1.14B revenue, $19.80 avg fare
--   2025: 44.2M trips, $1.27B revenue, $20.02 avg fare
--   2026: 6.7M trips,  $202M revenue,  $21.42 avg fare
--
--   Growth 2024 to 2025:
--   Trips:   +11.3%
--   Revenue: +12.1%
--   Avg fare: +1.1%
--
--   Declining tip %: 11.32% (2024) -> 10.06% (2025) -> 9.09% (2026)
--   Consistent downward trend — passengers tipping less each year
--   Speed stable ~11mph — NYC traffic conditions unchanged
-- ============================================================

SELECT
    f.pickup_year,
    COUNT(*)                                                AS total_trips,
    CAST(SUM(CAST(f.total_amount AS FLOAT)) AS DECIMAL(20,2)) AS total_revenue,
    CAST(AVG(f.fare_amount) AS DECIMAL(10,2))               AS avg_fare,
    CAST(AVG(f.tip_pct) AS DECIMAL(10,2))                   AS avg_tip_pct,
    CAST(AVG(f.trip_distance_miles) AS DECIMAL(10,2))        AS avg_distance,
    CAST(AVG(CAST(f.trip_duration_mins AS FLOAT))
        AS DECIMAL(10,2))                                   AS avg_duration,
    CAST(AVG(CAST(f.avg_speed_mph AS FLOAT))
        AS DECIMAL(10,2))                                   AS avg_speed
FROM gold.fact_trips f
GROUP BY f.pickup_year
ORDER BY f.pickup_year;