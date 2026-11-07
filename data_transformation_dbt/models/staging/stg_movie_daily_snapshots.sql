WITH source_data AS 
(
    SELECT
        CAST(id AS BIGINT) AS movie_source_id,
        CAST(vote_average AS DOUBLE) AS vote_average,
        CAST(vote_count AS INTEGER) AS vote_count,
        CAST(popularity AS DOUBLE) AS popularity,
        DATE_DIFF('day', CAST(release_date AS DATE), CURRENT_DATE) AS days_since_release,
        year,
        month,
        day
    FROM {{ source('movie_raw_lake', 'ext_raw_movie_daily_snapshot') }}
    WHERE release_date IS NOT NULL
    AND TRIM(release_date) != ''
)

SELECT * FROM source_data

























