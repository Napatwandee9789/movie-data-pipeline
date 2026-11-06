{{
    config(
        materialized = 'incremental',
        unique_key = 'movie_genre_bridge_id',
        incremental_strategy = 'merge',
        merge_update_columns = []
    )
}}

WITH 
{% if not is_incremental() %} -- Case 1: Full Refresh

    source_historical AS (
        SELECT
            dm.movie_source_id AS movie_source_id,
            mh.genres
        FROM {{ ref('stg_movie_historical') }} mh
        INNER JOIN {{ref('dim_movies')}} dm
        ON mh.movie_source_id = dm.movie_source_id
    ),

    source_daily AS (
        SELECT
            dm.movie_source_id AS movie_source_id,
            md.genres
        FROM {{ ref('stg_movie_daily_changes') }} md
        INNER JOIN {{ref('dim_movies')}} dm
        ON md.movie_source_id = dm.movie_source_id  
    ),

    movies_genres_historical AS (
        SELECT
            {{dbt_utils.generate_surrogate_key(['movie_source_id', 'g.id'])}} as movie_genre_bridge_id,
            {{dbt_utils.generate_surrogate_key(['movie_source_id',])}} as movie_sur_id,
            COALESCE({{dbt_utils.generate_surrogate_key(['dg.genre_source_id',])}}, '-2') AS genre_sur_id
        FROM source_historical
        CROSS JOIN UNNEST(genres) AS t(g)
        LEFT JOIN {{ref('dim_genres')}} dg
        ON g.id = dg.genre_source_id
    ),

    movies_genres_daily AS (
        SELECT
            {{dbt_utils.generate_surrogate_key(['movie_source_id', 'g.id'])}} as movie_genre_bridge_id,
            {{dbt_utils.generate_surrogate_key(['movie_source_id',])}} as movie_sur_id,
            COALESCE({{dbt_utils.generate_surrogate_key(['dg.genre_source_id',])}}, '-2') AS genre_sur_id
        FROM source_daily
        CROSS JOIN UNNEST(genres) AS t(g)
        LEFT JOIN {{ref('dim_genres')}} dg
        ON g.id = dg.genre_source_id
    ),

    merge_data AS
    (
        SELECT *
        FROM movies_genres_historical

        UNION

        SELECT *
        FROM movies_genres_daily
    )

    SELECT 
        *,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
    FROM merge_data

{% else %} -- Case 2: Incremental Load

    source_daily AS (
        SELECT
            dm.movie_source_id AS movie_source_id,
            md.genres
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
        {{dbt_utils.generate_surrogate_key(['movie_source_id', 'g.id'])}} as movie_genre_bridge_id,
        {{dbt_utils.generate_surrogate_key(['movie_source_id',])}} as movie_sur_id,
        COALESCE({{dbt_utils.generate_surrogate_key(['dg.genre_source_id',])}}, '-2') AS genre_sur_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
    FROM source_daily
    CROSS JOIN UNNEST(genres) AS t(g)
    LEFT JOIN {{ref('dim_genres')}} dg
    ON g.id = dg.genre_source_id

{% endif %}