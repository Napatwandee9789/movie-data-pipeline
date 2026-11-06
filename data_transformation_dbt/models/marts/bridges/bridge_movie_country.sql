{{
    config(
        materialized = 'incremental',
        unique_key = 'movie_country_bridge_id',
        incremental_strategy = 'merge',
        merge_update_columns = []
    )
}}

WITH 
{% if not is_incremental() %} -- Case 1: Full Refresh

    source_historical AS (
        SELECT
            dm.movie_source_id AS movie_source_id,
            mh.origin_country
        FROM {{ ref('stg_movie_historical') }} mh
        INNER JOIN {{ref('dim_movies')}} dm
        ON mh.movie_source_id = dm.movie_source_id  
    ),

    source_daily AS (
        SELECT
            dm.movie_source_id AS movie_source_id,
            md.origin_country
        FROM {{ ref('stg_movie_daily_changes') }} md
        INNER JOIN {{ref('dim_movies')}} dm
        ON md.movie_source_id = dm.movie_source_id      
    ),

    movies_countries_historical AS (
        SELECT
            movie_source_id,
            country_code AS iso_3166_1
        FROM source_historical
        CROSS JOIN UNNEST(origin_country) AS t(country_code)
    ),

    movies_countries_daily AS (
        SELECT
            movie_source_id,
            country_code AS iso_3166_1
        FROM source_daily
        CROSS JOIN UNNEST(origin_country) AS t(country_code)
    ),

    merge_data AS (
        SELECT DISTINCT movie_source_id, iso_3166_1
        FROM movies_countries_historical

        UNION

        SELECT DISTINCT movie_source_id, iso_3166_1
        FROM movies_countries_daily
    )

    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['m.movie_source_id', 'm.iso_3166_1']) }} AS movie_country_bridge_id,
        {{ dbt_utils.generate_surrogate_key(['m.movie_source_id']) }} AS movie_sur_id,
        COALESCE(dc.country_sur_id, '-2') AS country_sur_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
    FROM merge_data m
    LEFT JOIN {{ ref('dim_countries') }} dc
    ON m.iso_3166_1 = dc.iso_3166_1

{% else %} -- Case 2: Incremental Load

    source_daily AS (
        SELECT
            dm.movie_source_id AS movie_source_id,
            md.origin_country
        FROM {{ ref('stg_movie_daily_changes') }} md
        INNER JOIN {{ref('dim_movies')}} dm
        ON md.movie_source_id = dm.movie_source_id
        WHERE
            (year, month, day) IN (
                {% for i in range(0, 1) %}
                    (
                        '{{ (run_started_at - modules.datetime.timedelta(days=i)).strftime("%Y") }}',
                        '{{ (run_started_at - modules.datetime.timedelta(days=i)).strftime("%m") }}',
                        '{{ (run_started_at - modules.datetime.timedelta(days=i)).strftime("%d") }}'
                    ){{ "," if not loop.last }}
                {% endfor %}
            ) 
    )

    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['s.movie_source_id', 't.country_code']) }} AS movie_country_bridge_id,
        {{ dbt_utils.generate_surrogate_key(['s.movie_source_id']) }} AS movie_sur_id,
        COALESCE(dc.country_sur_id, '-2') AS country_sur_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
    FROM source_daily s
    CROSS JOIN UNNEST(s.origin_country) AS t(country_code)
    LEFT JOIN {{ ref('dim_countries') }} dc
    ON t.country_code = dc.iso_3166_1

{% endif %}