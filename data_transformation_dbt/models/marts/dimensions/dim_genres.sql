{{
    config(
        materialized="table"
    )
}}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['genre_source_id']) }} AS genre_sur_id,
    genre_source_id,
    genre
FROM {{ ref('stg_genres') }}

UNION ALL

SELECT 
    '-1' AS genre_sur_id,
    CAST(NULL AS INTEGER) AS genre_source_id,
    'None' AS genre

UNION ALL

SELECT 
    '-2' AS genre_sur_id,
    CAST(NULL AS INTEGER) AS genre_source_id,
    'Invalid' AS genre