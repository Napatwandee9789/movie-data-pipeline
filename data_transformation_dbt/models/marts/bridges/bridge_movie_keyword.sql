{{
    config(
        materialized = 'incremental',
        unique_key = 'movie_keyword_bridge_id',
        incremental_strategy = 'merge',
        merge_update_columns = [],
        post_hook = [
        "{% if execute and modules.datetime.date.today().weekday() == 6 %} OPTIMIZE {{ this }} REWRITE DATA USING BIN_PACK; {% endif %}",
        "{% if execute and modules.datetime.date.today().weekday() == 6 %} VACUUM {{ this }}; {% endif %}"
        ]
    )
}}

WITH 
{% if not is_incremental() %} -- Case 1: Full Refresh

    source_historical AS (
        SELECT
            dm.movie_source_id AS movie_source_id,
            mh.keywords
        FROM {{ ref('stg_movie_historical') }} mh
        INNER JOIN {{ ref('dim_movies') }} dm 
        ON mh.movie_source_id = dm.movie_source_id
    ),

    source_daily AS (
        SELECT
            dm.movie_source_id AS movie_source_id,
            md.keywords
        FROM {{ ref('stg_movie_daily_changes') }} md
        INNER JOIN {{ ref('dim_movies') }} dm
        ON md.movie_source_id = dm.movie_source_id    
    ),

    movies_keywords_historical AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['movie_source_id', 'kw.id']) }} AS movie_keyword_bridge_id,
            {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id,
            {{ dbt_utils.generate_surrogate_key(['kw.id']) }} AS keyword_sur_id
        FROM source_historical
        CROSS JOIN UNNEST(keywords.keywords) AS t(kw)
        INNER JOIN {{ ref('dim_keywords') }} dk
        ON kw.id = dk.keyword_source_id
    ),

    movies_keywords_daily AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['movie_source_id', 'kw.id']) }} AS movie_keyword_bridge_id,
            {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id,
            {{ dbt_utils.generate_surrogate_key(['kw.id']) }} AS keyword_sur_id
        FROM source_daily
        CROSS JOIN UNNEST(keywords.keywords) AS t(kw)
        INNER JOIN {{ ref('dim_keywords') }} dk
        ON kw.id = dk.keyword_source_id
    ),

    merge_data AS (
        SELECT *
        FROM movies_keywords_historical

        UNION

        SELECT *
        FROM movies_keywords_daily
    )

    SELECT 
        *,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
    FROM merge_data

{% else %} -- Case 2: Incremental Load

    source_daily AS (
        SELECT
            dm.movie_source_id AS movie_source_id,
            md.keywords
        FROM {{ ref('stg_movie_daily_changes') }} md
        INNER JOIN {{ ref('dim_movies') }} dm
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
        {{ dbt_utils.generate_surrogate_key(['movie_source_id', 'kw.id']) }} AS movie_keyword_bridge_id,
        {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id,
        {{ dbt_utils.generate_surrogate_key(['kw.id']) }} AS keyword_sur_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
    FROM source_daily
    CROSS JOIN UNNEST(keywords.keywords) AS t(kw)
    INNER JOIN {{ ref('dim_keywords') }} dk
    ON kw.id = dk.keyword_source_id

{% endif %}