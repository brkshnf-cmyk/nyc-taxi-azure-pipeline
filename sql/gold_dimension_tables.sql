-- ============================================================
-- NYC Yellow Taxi — Gold Layer Dimension Tables
-- File: gold_dimension_tables.sql
-- Description: Creates all dimension tables for the star schema
--              gold layer. Dimensions translate numeric codes
--              from source data into meaningful business labels.
--              All dimensions implemented as views — no data
--              duplication, definitions live in SQL only.
-- Run against: Synapse Serverless SQL Pool — nyc_taxi_db
-- Dependencies: src_bronze_taxi (external data source)
--               silver.vw_yellow_taxi (silver layer view)
-- ============================================================


-- ============================================================
-- STEP 1: Create Gold Schema
-- Purpose: Separate namespace for business-ready aggregated
--          objects. Bronze = raw, Silver = clean, Gold = ready.
-- Note: Skip if schema already exists.
-- ============================================================

CREATE SCHEMA gold;


-- ============================================================
-- STEP 2: dim_vendor
-- Purpose: Translates VendorID numeric codes into vendor names.
--          TLC licenses two vendors to provide taxi technology:
--          1 = Creative Mobile Technologies (CMT)
--          2 = VeriFone Inc (VTS)
--          3 = Unknown (safety fallback for any unlisted codes)
-- Rows: 3
-- ============================================================

CREATE OR ALTER VIEW gold.dim_vendor AS
SELECT *
FROM (VALUES
    (1, 'Creative Mobile Technologies', 'CMT'),
    (2, 'VeriFone Inc',                 'VTS'),
    (3, 'Unknown',                       'UNK')
) AS v(vendor_id, vendor_name, vendor_code);


-- ============================================================
-- STEP 3: dim_payment_type
-- Purpose: Translates payment_type numeric codes into
--          meaningful payment method descriptions.
--          Source: TLC data dictionary
--          1 = Credit Card (most common — ~70% of trips)
--          2 = Cash
--          3 = No Charge (complimentary trips)
--          4 = Dispute (fare disputed by passenger)
--          5 = Unknown
--          6 = Voided Trip
-- Rows: 6
-- ============================================================

CREATE OR ALTER VIEW gold.dim_payment_type AS
SELECT *
FROM (VALUES
    (1, 'Credit Card',  'CC'),
    (2, 'Cash',         'CASH'),
    (3, 'No Charge',    'NC'),
    (4, 'Dispute',      'DISP'),
    (5, 'Unknown',      'UNK'),
    (6, 'Voided Trip',  'VOID')
) AS p(payment_type_id, payment_description, payment_code);


-- ============================================================
-- STEP 4: dim_rate_code
-- Purpose: Translates RatecodeID numeric codes into rate type
--          descriptions. Rate codes determine how the fare
--          is calculated for each trip.
--          1 = Standard Rate (vast majority of trips)
--          2 = JFK Airport (flat rate $70 from/to JFK)
--          3 = Newark Airport (metered + $20 surcharge)
--          4 = Nassau/Westchester (outside NYC)
--          5 = Negotiated Fare (pre-arranged price)
--          6 = Group Ride (shared ride)
--         99 = Unknown (safety fallback)
-- Rows: 7
-- ============================================================

CREATE OR ALTER VIEW gold.dim_rate_code AS
SELECT *
FROM (VALUES
    (1,  'Standard Rate',        'STD'),
    (2,  'JFK',                  'JFK'),
    (3,  'Newark',               'EWR'),
    (4,  'Nassau/Westchester',   'NAS'),
    (5,  'Negotiated Fare',      'NEG'),
    (6,  'Group Ride',           'GRP'),
    (99, 'Unknown',              'UNK')
) AS r(rate_code_id, rate_description, rate_code);


-- ============================================================
-- STEP 5: dim_location
-- Purpose: Maps TLC LocationID (1-265) to taxi zone names,
--          boroughs and service zones. Source is the official
--          TLC taxi zone lookup CSV stored in the bronze
--          reference layer.
--
--          File path in ADLS:
--          bronze/yellow-taxi/reference/taxi-zones/taxi_zone_lookup.csv
--
--          UTF8 collation required — borough/zone names contain
--          special characters. Latin1_General_100_BIN2_UTF8
--          handles this correctly.
--
--          Key borough values: Manhattan, Brooklyn, Queens,
--          Bronx, Staten Island, EWR (Newark Airport)
-- Rows: 265
-- ============================================================

CREATE OR ALTER VIEW gold.dim_location AS
SELECT
    CAST(LocationID AS SMALLINT)    AS location_id,
    Borough                          AS borough,
    Zone                             AS zone_name,
    service_zone                     AS service_zone
FROM OPENROWSET(
    BULK 'reference/taxi-zones/taxi_zone_lookup.csv',
    DATA_SOURCE = 'src_bronze_taxi',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
) WITH (
    LocationID      INT,
    Borough         VARCHAR(50)  COLLATE Latin1_General_100_BIN2_UTF8,
    Zone            VARCHAR(100) COLLATE Latin1_General_100_BIN2_UTF8,
    service_zone    VARCHAR(50)  COLLATE Latin1_General_100_BIN2_UTF8
) AS locations;


-- ============================================================
-- STEP 6: Verify All Dimension Tables
-- Purpose: Confirm all dimensions return correct row counts
--          and expected values before building fact table.
-- Expected: vendor=3, payment=6, rate=7, location=265
-- ============================================================

-- Verify dim_vendor
SELECT * FROM gold.dim_vendor;

-- Verify dim_payment_type
SELECT * FROM gold.dim_payment_type;

-- Verify dim_rate_code
SELECT * FROM gold.dim_rate_code;

-- Verify dim_location row count — should be 265
SELECT COUNT(*) AS location_count
FROM gold.dim_location;

-- Sample dim_location rows
SELECT TOP 10 *
FROM gold.dim_location
ORDER BY location_id;


-- ============================================================
-- STEP 7: Test Dimension Joins to Silver View
-- Purpose: Validate that all dimension tables join correctly
--          to the silver view, returning meaningful labels
--          instead of numeric codes.
--
-- Filter: is_valid_trip = 1 AND is_outlier = 0
--         This is the standard gold layer filter — only
--         clean, non-outlier trips used for analysis.
--
-- Key observations from results:
--   - Manhattan dominates pickups and dropoffs
--   - Credit Card is the most common payment (~70%)
--   - VeriFone Inc handles more trips than CMT
--   - Standard Rate applies to the vast majority of trips
--   - Average fares $15-20, trip durations 8-15 minutes
-- ============================================================

SELECT TOP 100
    t.pickup_datetime,
    t.dropoff_datetime,
    t.trip_duration_mins,
    t.trip_distance_miles,
    t.fare_amount,
    t.tip_amount,
    t.total_amount,
    v.vendor_name,
    p.payment_description,
    r.rate_description,
    pu.zone_name        AS pickup_zone,
    pu.borough          AS pickup_borough,
    do.zone_name        AS dropoff_zone,
    do.borough          AS dropoff_borough
FROM silver.vw_yellow_taxi t
LEFT JOIN gold.dim_vendor       v  ON t.vendor_id          = v.vendor_id
LEFT JOIN gold.dim_payment_type p  ON t.payment_type_id    = p.payment_type_id
LEFT JOIN gold.dim_rate_code    r  ON t.rate_code_id        = r.rate_code_id
LEFT JOIN gold.dim_location     pu ON t.pickup_location_id  = pu.location_id
LEFT JOIN gold.dim_location     do ON t.dropoff_location_id = do.location_id
WHERE t.is_valid_trip = 1
AND   t.is_outlier    = 0;