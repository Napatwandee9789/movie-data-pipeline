WITH source_data AS 
(
    SELECT
        CAST(id AS INTEGER) AS genre_source_id, 
        CAST(name AS VARCHAR) AS genre
    FROM {{ source('movie_raw_lake', 'ext_raw_genres') }}
)

SELECT * FROM source_data