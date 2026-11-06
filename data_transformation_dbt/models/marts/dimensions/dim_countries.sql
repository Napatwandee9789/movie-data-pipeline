{{
    config(
        materialized="table"
    )
}}

SELECT 
    {{dbt_utils.generate_surrogate_key(['iso_3166_1'])}} as country_sur_id,
    country_name,
    iso_3166_1
FROM {{ref('stg_countries')}}

UNION ALL

SELECT
    '-1' AS country_sur_id,
    'None' AS country_name,
    'None' AS iso_3166_1

UNION ALL

SELECT
    '-2' AS country_sur_id,
    'Invalid' AS country_name,
    'Invalid' AS iso_3166_1


