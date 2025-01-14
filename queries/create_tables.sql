
-- Create table for season calendar dates
DROP TABLE IF EXISTS dim_season_calendar;
DROP TABLE IF EXISTS raw_injured_reserve;
DROP TABLE IF EXISTS raw_injuries;

CREATE TABLE dim_season_calendar
(
    season INTEGER
    , week_number INTEGER
    , report_date DATE
    , week_start_date DATE
    , week_end_date DATE
)
;

CREATE TABLE IF NOT EXISTS raw_injured_reserve
(
date_added DATE
, season INTEGER
, report_date DATE
, team TEXT
, acquired TEXT
, relinquished TEXT
, notes TEXT
);

CREATE TABLE IF NOT EXISTS raw_injuries
(
date_added DATE
, season INTEGER
, report_date DATE
, team TEXT
, acquired TEXT
, relinquished TEXT
, notes TEXT
);

-- CREATE TABLE injuries AS
-- SELECT 
-- 2023 AS season
-- , DATE('2023-09-07 00:00:00') AS report_date
-- UNION ALL
-- SELECT
-- 2023 AS season
-- , DATE('2024-01-01 00:00:00') AS report_date
-- UNION ALL
-- SELECT 
-- 2024 AS season
-- , DATE('2024-09-05 00:00:00') AS report_date
-- UNION ALL
-- SELECT
-- 2024 AS season
-- , DATE('2024-10-31 00:00:00') AS report_date
-- ;

