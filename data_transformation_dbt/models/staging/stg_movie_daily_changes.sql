WITH source_data AS 
(
    SELECT
        CAST(id AS BIGINT) AS movie_source_id,
        CAST(budget AS BIGINT) AS budget,
        CAST(revenue AS BIGINT) AS revenue,
        CAST(runtime AS INTEGER) AS runtime,
        genres, -- did not unnest yet
        CAST(status AS VARCHAR) AS status,
        origin_country, -- did not unnest yet
        CAST(original_language AS VARCHAR) AS original_language,
        CAST(original_title AS VARCHAR) AS original_title,
        CAST(title AS VARCHAR) AS title,
        CAST(overview AS VARCHAR) AS overview,
        CAST(poster_path AS VARCHAR) AS poster_path,
        CAST(backdrop_path AS VARCHAR) AS backdrop_path,
        TRY_CAST(NULLIF(TRIM(CAST(release_date AS VARCHAR)), '') AS DATE) AS release_date,
        keywords, -- did not unnest yet
        credits,  -- did not unnest yet
        production_companies,  -- did not unnest yet
        year,
        month,
        day
    FROM {{ source('movie_raw_lake', 'ext_raw_movie_daily_changes') }}
    WHERE release_date >= '2016-01-01' -- filter out invalid release_date
    AND CAST(release_date AS DATE) <= CURRENT_DATE -- filter out future release_date
)

SELECT * FROM source_data


        

        































