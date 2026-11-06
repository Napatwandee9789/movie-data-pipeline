{{
    config(
        materialized = 'incremental',
        unique_key = 'movie_company_bridge_id',
        incremental_strategy = 'merge',
        merge_update_columns = []
    )
}}

WITH 
{% if not is_incremental() %} -- Case 1: Full Refresh

    source_historical AS (
        SELECT
            mh.movie_source_id,
            mh.production_companies
        FROM {{ ref('stg_movie_historical') }} mh
        INNER JOIN {{ref('dim_movies')}} dm
        ON mh.movie_source_id = dm.movie_source_id
        WHERE production_companies IS NOT NULL
    ),

    source_daily AS (
        SELECT
            md.movie_source_id,
            md.production_companies
        FROM {{ ref('stg_movie_daily_changes') }} md
        INNER JOIN {{ref('dim_movies')}} dm
        ON md.movie_source_id = dm.movie_source_id  
        WHERE production_companies IS NOT NULL
    ),

    movies_companies_historical AS (
        SELECT
            movie_source_id,
            pc.id AS company_source_id
        FROM source_historical
        CROSS JOIN UNNEST(production_companies) AS t(pc)
        WHERE pc.id IS NOT NULL
    ),

    movies_companies_daily AS (
        SELECT
            movie_source_id,
            pc.id AS company_source_id
        FROM source_daily
        CROSS JOIN UNNEST(production_companies) AS t(pc)
        WHERE pc.id IS NOT NULL
    ),

    merge_data AS (
        SELECT DISTINCT 
            movie_source_id,
            company_source_id,
            {{ dbt_utils.generate_surrogate_key(['company_source_id']) }} AS company_sur_id,
            {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id
        FROM movies_companies_historical

        UNION

        SELECT DISTINCT 
            movie_source_id,
            company_source_id,
            {{ dbt_utils.generate_surrogate_key(['company_source_id']) }} AS company_sur_id,
            {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id
        FROM movies_companies_daily
    )

    SELECT
        {{ dbt_utils.generate_surrogate_key(['m.movie_source_id', 'm.company_source_id']) }} AS movie_company_bridge_id,
        m.movie_sur_id,
        dc.company_sur_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
    FROM merge_data m
    INNER JOIN {{ref('dim_companies')}} dc
    ON m.company_sur_id = dc.company_sur_id

{% else %} -- Case 2: Incremental Load

    source_daily AS (
        SELECT
            md.movie_source_id,
            md.production_companies
        FROM {{ ref('stg_movie_daily_changes') }} md
        INNER JOIN {{ref('dim_movies')}} dm
        ON md.movie_source_id = dm.movie_source_id  
        WHERE
            production_companies IS NOT NULL
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

    unpacked_daily AS (
        SELECT
            movie_source_id,
            pc.id AS company_source_id,
            {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id,
            {{ dbt_utils.generate_surrogate_key(['pc.id']) }} AS company_sur_id
        FROM source_daily
        CROSS JOIN UNNEST(production_companies) AS t(pc)
        WHERE pc.id IS NOT NULL
    )

    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['ud.movie_source_id', 'ud.company_source_id']) }} AS movie_company_bridge_id,
        ud.movie_sur_id,
        dc.company_sur_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
    FROM unpacked_daily ud
    INNER JOIN {{ref('dim_companies')}} dc
    ON ud.company_sur_id = dc.company_sur_id
    

{% endif %}