{{
    config(
        materialized = 'incremental',
        unique_key = 'keyword_sur_id',
        incremental_strategy = 'merge',
        merge_update_columns = []
    )
}}

WITH 
{% if not is_incremental() %} -- Case 1: Full Refresh

    source_historical AS (
        SELECT
            mh.movie_source_id,
            mh.keywords
        FROM {{ ref('stg_movie_historical') }} mh
        INNER JOIN {{ref('dim_movies')}} dm
        ON mh.movie_source_id = dm.movie_source_id
        WHERE keywords IS NOT NULL
    ),

    source_daily AS (
        SELECT
            md.movie_source_id,
            md.keywords
        FROM {{ ref('stg_movie_daily_changes') }} md
        INNER JOIN {{ref('dim_movies')}} dm
        ON md.movie_source_id = dm.movie_source_id    
        WHERE keywords IS NOT NULL
    ),

    keywords_historical AS (
        SELECT
            kw.id AS keyword_source_id,
            kw.name AS keyword
        FROM source_historical
        CROSS JOIN UNNEST(keywords.keywords) AS t(kw)
        WHERE kw.id IS NOT NULL
    ),

    keywords_daily AS (
        SELECT
            kw.id AS keyword_source_id,
            kw.name AS keyword
        FROM source_daily
        CROSS JOIN UNNEST(keywords.keywords) AS t(kw)
        WHERE kw.id IS NOT NULL
    ),

    combined_keywords AS (
        SELECT * FROM keywords_historical
        UNION ALL
        SELECT * FROM keywords_daily
    ),

    deduped_keywords AS (
        SELECT 
            keyword_source_id,
            keyword,
            ROW_NUMBER() OVER (
                PARTITION BY keyword_source_id 
                ORDER BY keyword DESC
            ) AS rn
        FROM combined_keywords
    )

    SELECT 
        {{ dbt_utils.generate_surrogate_key(['keyword_source_id']) }} AS keyword_sur_id,
        keyword_source_id,
        keyword,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
    FROM deduped_keywords
    WHERE rn = 1

{% else %} -- Case 2: Incremental Load

    source_daily AS (
        SELECT
            md.movie_source_id,
            md.keywords
        FROM {{ ref('stg_movie_daily_changes') }} md
        INNER JOIN {{ref('dim_movies')}} dm
        ON md.movie_source_id = dm.movie_source_id       
        WHERE
            keywords IS NOT NULL
            AND (year, month, day) IN (
                {% for i in range(0, 1) %}
                    (
                        '{{ (run_started_at - modules.datetime.timedelta(days=i)).strftime("%Y") }}',
                        '{{ (run_started_at - modules.datetime.timedelta(days=i)).strftime("%m") }}',
                        '{{ (run_started_at - modules.datetime.timedelta(days=i)).strftime("%d") }}'
                    ){{ "," if not loop.last }}
                {% endfor %}
            ) 
    ),

    unpacked_keywords AS (
        SELECT
            kw.id AS keyword_source_id,
            kw.name AS keyword
        FROM source_daily
        CROSS JOIN UNNEST(keywords.keywords) AS t(kw)
        WHERE kw.id IS NOT NULL
    ),

    deduped_keywords AS (
        SELECT
            keyword_source_id,
            keyword,
            ROW_NUMBER() OVER (
                PARTITION BY keyword_source_id 
                ORDER BY keyword DESC
            ) AS rn
        FROM unpacked_keywords
    )

    SELECT 
        {{ dbt_utils.generate_surrogate_key(['keyword_source_id']) }} AS keyword_sur_id,
        keyword_source_id,
        keyword,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
    FROM deduped_keywords
    WHERE rn = 1

{% endif %}