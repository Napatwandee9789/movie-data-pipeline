{{ config(
    materialized='table'
) }}

WITH date_array AS (
    SELECT CAST(date AS DATE) AS date
    FROM UNNEST(SEQUENCE(DATE '2016-01-01', DATE '2030-12-31', INTERVAL '1' DAY)) t(date)
),

calendar_base AS (
    SELECT
        CAST(DATE_FORMAT(date, '%Y%m%d') AS BIGINT) AS date_sur_id,
        CAST(date AS DATE) AS full_date,
        CAST(EXTRACT(YEAR FROM date) AS BIGINT) AS year,
        CAST(EXTRACT(QUARTER FROM date) AS BIGINT) AS quarter,
        CAST(EXTRACT(MONTH FROM date) AS BIGINT) AS month,
        CAST(EXTRACT(DAY FROM date) AS BIGINT) AS day
    FROM date_array
)

SELECT 
    CAST(-1 AS BIGINT) AS date_sur_id,
    CAST(NULL AS DATE) AS full_date,
    CAST(NULL AS BIGINT) AS year,
    CAST(NULL AS BIGINT) AS quarter,
    CAST(NULL AS BIGINT) AS month,
    CAST(NULL AS BIGINT) AS day

UNION ALL

SELECT 
    date_sur_id,
    full_date,
    year,
    quarter,
    month,
    day
FROM calendar_base