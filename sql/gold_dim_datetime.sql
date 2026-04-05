-- ============================================================
-- NYC Yellow Taxi — Gold Layer dim_datetime & Time Analysis
-- File: gold_dim_datetime.sql
-- Description: Creates the datetime dimension view for the
--              star schema gold layer. Provides time-based
--              attributes and flags for Power BI time
--              intelligence. Also includes direct analysis
--              queries that bypass the join for performance.
-- Run against: Synapse Serverless SQL Pool — nyc_taxi_db
-- Dependencies: silver.vw_yellow_taxi
-- Cost note:   Each full scan costs ~$0.015 (1.5 cents)
--              Total budget safe up to ~666 full scans
-- ============================================================


-- ============================================================
-- STEP 1: Create dim_datetime View
-- Purpose: One row per distinct hour across the full dataset.
--          Pre-calculates time attributes and flags so Power BI
--          can do time intelligence without complex DAX.
--
-- Design note: dim_datetime is derived from silver view pickup
--              timestamps. Primary use is Power BI relationship
--              model — not for SQL joins on 90M rows (expensive).
--              For ad-hoc SQL analysis use direct DATEPART()
--              calculations instead (see Steps 2-5 below).
--
-- Flags defined:
--   is_weekend   = 1 for Sunday(1) and Saturday(7)
--   is_peak_hour = 1 for weekday 7-9am and 4-7pm
--   is_late_night= 1 for 10pm to 4am any day
--   month_period = Early (1-10) / Mid (11-20) / Late (21-31)
-- ============================================================

CREATE OR ALTER VIEW gold.dim_datetime AS
SELECT DISTINCT
    -- =========================================
    -- PRIMARY KEY
    -- Truncated to hour — one row per hour
    -- =========================================
    DATEADD(HOUR,
        DATEDIFF(HOUR, 0, pickup_datetime), 0)      AS datetime_id,

    -- =========================================
    -- DATE COMPONENTS
    -- =========================================
    CAST(pickup_datetime AS DATE)                    AS date_actual,
    YEAR(pickup_datetime)                            AS year,
    MONTH(pickup_datetime)                           AS month_num,
    DAY(pickup_datetime)                             AS day_num,
    DATEPART(HOUR, pickup_datetime)                  AS hour_num,
    DATEPART(QUARTER, pickup_datetime)               AS quarter_num,
    DATEPART(WEEK, pickup_datetime)                  AS week_num,
    DATEPART(dw, pickup_datetime)                    AS day_of_week_num,

    -- =========================================
    -- DATE LABELS
    -- =========================================
    DATENAME(MONTH, pickup_datetime)                 AS month_name,
    DATENAME(WEEKDAY, pickup_datetime)               AS day_name,
    CONCAT('Q', DATEPART(QUARTER, pickup_datetime))  AS quarter_label,
    CONCAT(
        YEAR(pickup_datetime), '-',
        RIGHT('0' + CAST(MONTH(pickup_datetime)
            AS VARCHAR(2)), 2))                      AS year_month,

    -- =========================================
    -- FLAGS
    -- =========================================

    -- Weekend: 1=Sunday, 7=Saturday in Synapse
    CASE WHEN DATEPART(dw, pickup_datetime)
             IN (1, 7) THEN 1 ELSE 0
         END                                         AS is_weekend,

    -- Peak: weekday 7-9am and 4-7pm
    CASE WHEN DATEPART(dw, pickup_datetime)
             NOT IN (1, 7)
         AND (
             DATEPART(HOUR, pickup_datetime) BETWEEN 7 AND 9
          OR DATEPART(HOUR, pickup_datetime) BETWEEN 16 AND 19
         )
         THEN 1 ELSE 0
         END                                         AS is_peak_hour,

    -- Late night: 10pm to 4am
    CASE WHEN DATEPART(HOUR, pickup_datetime) >= 22
          OR  DATEPART(HOUR, pickup_datetime) < 4
         THEN 1 ELSE 0
         END                                         AS is_late_night,

    -- Month period
    CASE WHEN DAY(pickup_datetime) <= 10  THEN 'Early'
         WHEN DAY(pickup_datetime) <= 20  THEN 'Mid'
         ELSE 'Late'
         END                                         AS month_period

FROM silver.vw_yellow_taxi
WHERE is_valid_trip = 1;


-- ============================================================
-- STEP 2: Trips by Hour of Day
-- Purpose: Understand demand patterns across 24 hours.
--          Use direct DATEPART() — faster than joining
--          dim_datetime on 90M rows.
--
-- Findings:
--   Busiest:  18:00 (6pm)  — 6,248,202 trips, avg fare $18.79
--   Quietest: 04:00 (4am)  — 602,254 trips,   avg fare $23.67
--   Highest fare:  05:00   — $26.71 (airport runs, long trips)
--   Best tippers:  16:00   — $3.73 avg tip (business travellers)
--   Pattern: demand builds from 6am, peaks 6pm, drops after 11pm
-- ============================================================

SELECT
    DATEPART(HOUR, pickup_datetime)         AS hour_num,
    COUNT(*)                                AS total_trips,
    AVG(fare_amount)                        AS avg_fare,
    AVG(tip_amount)                         AS avg_tip
FROM silver.vw_yellow_taxi
WHERE is_valid_trip = 1
AND   is_outlier    = 0
GROUP BY DATEPART(HOUR, pickup_datetime)
ORDER BY hour_num;


-- ============================================================
-- STEP 3: Trips by Day of Week
-- Purpose: Identify which days drive most demand and revenue.
--
-- Findings:
--   Busiest:       Thursday — 14,059,250 trips
--   Also busy:     Saturday — 14,058,202 trips
--   Quietest:      Monday   — 11,009,186 trips
--   Highest fare:  Sunday   — $20.80 avg (leisure, longer trips)
--   Lowest fare:   Saturday — $19.21 avg (shorter leisure hops)
--   Pattern: mid-week and Saturday dominate volume,
--            Sunday and Monday have longest/most expensive trips
-- ============================================================

SELECT
    DATENAME(WEEKDAY, pickup_datetime)      AS day_name,
    DATEPART(dw, pickup_datetime)           AS day_of_week_num,
    COUNT(*)                                AS total_trips,
    AVG(fare_amount)                        AS avg_fare
FROM silver.vw_yellow_taxi
WHERE is_valid_trip = 1
AND   is_outlier    = 0
GROUP BY
    DATENAME(WEEKDAY, pickup_datetime),
    DATEPART(dw, pickup_datetime)
ORDER BY day_of_week_num;


-- ============================================================
-- STEP 4: Peak vs Off-Peak vs Late Night
-- Purpose: Compare trip volumes, fares and tips across
--          three time periods.
--
-- Findings:
--   Off Peak:   50,030,353 trips — avg fare $20.33, tip $3.18
--   Peak:       25,197,983 trips — avg fare $19.42, tip $3.34
--   Late Night: 15,369,909 trips — avg fare $20.05, tip $2.83
--
--   Key insights:
--   - Off peak dominates — taxis used all day not just rush hour
--   - Peak trips are shorter and cheaper (city commuters)
--   - Late night trips are longest (avg 3.78 miles)
--   - Peak riders tip most despite shortest trips
--     (likely business travellers expensing rides)
-- ============================================================

SELECT
    CASE
        WHEN DATEPART(dw, pickup_datetime) NOT IN (1,7)
         AND (DATEPART(HOUR, pickup_datetime) BETWEEN 7 AND 9
          OR  DATEPART(HOUR, pickup_datetime) BETWEEN 16 AND 19)
        THEN 'Peak'
        WHEN DATEPART(HOUR, pickup_datetime) >= 22
          OR DATEPART(HOUR, pickup_datetime) < 4
        THEN 'Late Night'
        ELSE 'Off Peak'
    END                                     AS time_period,
    COUNT(*)                                AS total_trips,
    AVG(fare_amount)                        AS avg_fare,
    AVG(tip_amount)                         AS avg_tip,
    AVG(trip_distance_miles)                AS avg_distance
FROM silver.vw_yellow_taxi
WHERE is_valid_trip = 1
AND   is_outlier    = 0
GROUP BY
    CASE
        WHEN DATEPART(dw, pickup_datetime) NOT IN (1,7)
         AND (DATEPART(HOUR, pickup_datetime) BETWEEN 7 AND 9
          OR  DATEPART(HOUR, pickup_datetime) BETWEEN 16 AND 19)
        THEN 'Peak'
        WHEN DATEPART(HOUR, pickup_datetime) >= 22
          OR DATEPART(HOUR, pickup_datetime) < 4
        THEN 'Late Night'
        ELSE 'Off Peak'
    END
ORDER BY total_trips DESC;


-- ============================================================
-- STEP 5: Weekend vs Weekday
-- Purpose: Compare leisure vs business travel patterns.
--
-- Findings:
--   Weekday: 64,746,884 trips — avg fare $20.07, tip $3.29
--   Weekend: 25,851,361 trips — avg fare $19.93, tip $2.87
--
--   Key insights:
--   - Weekdays have 2.5x more trips — primarily commuter tool
--   - Weekend trips slightly longer (3.55 vs 3.42 miles)
--   - Weekday tippers more generous ($3.29 vs $2.87)
--     business expense accounts vs personal spending
--   - Fares similar — distance and route differences cancel out
-- ============================================================

SELECT
    CASE WHEN DATEPART(dw, pickup_datetime) IN (1,7)
         THEN 'Weekend' ELSE 'Weekday'
         END                                AS day_type,
    COUNT(*)                                AS total_trips,
    AVG(fare_amount)                        AS avg_fare,
    AVG(tip_amount)                         AS avg_tip,
    AVG(trip_distance_miles)                AS avg_distance
FROM silver.vw_yellow_taxi
WHERE is_valid_trip = 1
AND   is_outlier    = 0
GROUP BY
    CASE WHEN DATEPART(dw, pickup_datetime) IN (1,7)
         THEN 'Weekend' ELSE 'Weekday'
         END
ORDER BY day_type;


-- ============================================================
-- SUMMARY OF FINDINGS — NYC Yellow Taxi Time Patterns
-- ============================================================
-- NYC Yellow Taxis are primarily a weekday business commuter
-- service. Key patterns:
--
-- DEMAND:    Peaks at 6pm (6.2M trips), quietest at 4am (602K)
-- BUSIEST:   Thursday and Saturday virtually tied (~14M each)
-- QUIETEST:  Monday — start of week avoidance
-- FARES:     Early morning (4-7am) most expensive — airport runs
-- TIPS:      Peak hour riders tip most — business travellers
-- DISTANCE:  Late night trips longest — leisure/social travel
-- WEEKDAY:   2.5x more trips than weekend
-- INSIGHT:   Off-peak has MORE trips than peak — taxis are an
--            all-day service, not just a rush hour one
-- ============================================================