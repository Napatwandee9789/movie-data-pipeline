WITH source_data AS 
(
    SELECT
        CAST(english_name AS VARCHAR) AS country_name,
        CAST(iso_3166_1 AS VARCHAR) AS iso_3166_1
    FROM {{ source('movie_raw_lake', 'ext_raw_countries') }}
)

SELECT * FROM source_data