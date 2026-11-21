{{
    config(
        materialized = 'incremental',
        unique_key = 'company_sur_id',
        incremental_strategy = 'merge',
        merge_update_columns = ['updated_at', 'company_name', 'logo_path'],
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
            mh.movie_source_id,
            mh.production_companies
        FROM {{ ref('stg_movie_historical') }} mh
        INNER JOIN {{ref('dim_movies')}} dm
        ON mh.movie_source_id = dm.movie_source_id
    ),

    source_daily AS (
        SELECT
            md.movie_source_id,
            md.production_companies
        FROM {{ ref('stg_movie_daily_changes') }} md
        INNER JOIN {{ref('dim_movies')}} dm
        ON md.movie_source_id = dm.movie_source_id  
    ),

    companies_historical AS (
        SELECT
            pc.id AS company_source_id,
            pc.name AS company_name,
            pc.logo_path AS logo_path
        FROM source_historical
        CROSS JOIN UNNEST(production_companies) AS t(pc)
    ),

    companies_daily AS (
        SELECT
            pc.id AS company_source_id,
            pc.name AS company_name,
            pc.logo_path AS logo_path
        FROM source_daily
        CROSS JOIN UNNEST(production_companies) AS t(pc)
    ),

    merge_data AS (
        SELECT * FROM companies_historical
        UNION ALL
        SELECT * FROM companies_daily
    ),

    deduped_companies AS (
        SELECT
            company_source_id,
            company_name,
            logo_path,
            ROW_NUMBER() OVER (
                PARTITION BY company_source_id 
                ORDER BY logo_path NULLS LAST, company_name DESC
            ) AS rn
        FROM merge_data
        WHERE company_source_id IS NOT NULL
    )

    SELECT 
        {{ dbt_utils.generate_surrogate_key(['company_source_id']) }} AS company_sur_id,
        company_source_id,
        company_name,
        logo_path,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS updated_at 
    FROM deduped_companies
    WHERE rn = 1

{% else %} -- Case 2: Incremental Load

    source_daily AS (
        SELECT
            md.movie_source_id,
            md.production_companies
        FROM {{ ref('stg_movie_daily_changes') }} md
        INNER JOIN {{ref('dim_movies')}} dm
        ON md.movie_source_id = dm.movie_source_id      
        WHERE
            (year, month, day) IN (
                {% for i in range(0, 1) %}
                    (
                        '{{ (run_started_at - modules.datetime.timedelta(days=i+1)).strftime("%Y") }}',
                        '{{ (run_started_at - modules.datetime.timedelta(days=i+1)).strftime("%m") }}',
                        '{{ (run_started_at - modules.datetime.timedelta(days=i+1)).strftime("%d") }}'
                    ){{ "," if not loop.last }}
                {% endfor %}
            ) 
    ),

    flattened_daily AS (
        SELECT
            pc.id AS company_source_id,
            pc.name AS company_name,
            pc.logo_path AS logo_path
        FROM source_daily
        CROSS JOIN UNNEST(production_companies) AS t(pc)
    ),

    deduped_daily AS (
        SELECT
            company_source_id,
            company_name,
            logo_path,
            ROW_NUMBER() OVER (
                PARTITION BY company_source_id 
                ORDER BY logo_path NULLS LAST, company_name DESC
            ) AS rn
        FROM flattened_daily
        WHERE company_source_id IS NOT NULL
    )

    SELECT 
        {{ dbt_utils.generate_surrogate_key(['company_source_id']) }} AS company_sur_id,
        company_source_id,
        company_name,
        logo_path,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS updated_at 
    FROM deduped_daily
    WHERE rn = 1

{% endif %}