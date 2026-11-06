{{
    config(
        materialized = 'incremental',
        unique_key = 'person_sur_id',
        incremental_strategy = 'merge',
        merge_update_columns = ['updated_at', 'name', 'gender', 'profile_path']
    )
}}

WITH 
{% if not is_incremental() %}

    source_historical AS (
        SELECT
            mh.movie_source_id AS movie_source_id,
            mh.credits
        FROM {{ ref('stg_movie_historical') }} mh
        INNER JOIN {{ref('dim_movies')}} dm
        ON mh.movie_source_id = dm.movie_source_id
    ),

    source_daily AS (
        SELECT
            md.movie_source_id,
            md.credits
        FROM {{ ref('stg_movie_daily_changes') }} md
        INNER JOIN {{ref('dim_movies')}} dm
        ON md.movie_source_id = dm.movie_source_id      
    ),

    casts_historical AS (
        SELECT
            c.id AS person_source_id,
            c.name AS name,
            c.gender AS gender,
            c.profile_path AS profile_path
        FROM source_historical
        CROSS JOIN UNNEST(credits."cast") AS t(c)
        WHERE c.id IS NOT NULL
    ),

    casts_daily AS (
        SELECT
            c.id AS person_source_id,
            c.name AS name,
            c.gender AS gender,
            c.profile_path AS profile_path
        FROM source_daily
        CROSS JOIN UNNEST(credits."cast") AS t(c)
        WHERE c.id IS NOT NULL
    ),

    crews_historical AS (
        SELECT
            cr.id AS person_source_id,
            cr.name AS name,
            cr.gender AS gender,
            cr.profile_path AS profile_path
        FROM source_historical
        CROSS JOIN UNNEST(credits.crew) AS t(cr)
        WHERE cr.job = 'Director' AND cr.id IS NOT NULL
    ),

    crews_daily AS (
        SELECT
            cr.id AS person_source_id,
            cr.name AS name,
            cr.gender AS gender,
            cr.profile_path AS profile_path
        FROM source_daily
        CROSS JOIN UNNEST(credits.crew) AS t(cr)
        WHERE cr.job = 'Director' AND cr.id IS NOT NULL
    ),

    merge_data AS (
        SELECT * FROM casts_historical
        UNION ALL
        SELECT * FROM casts_daily
        UNION ALL
        SELECT * FROM crews_historical
        UNION ALL
        SELECT * FROM crews_daily
    ),

    deduped_persons AS (
        SELECT
            person_source_id,
            name,
            gender,
            profile_path,
            ROW_NUMBER() OVER (
                PARTITION BY person_source_id 
                ORDER BY profile_path NULLS LAST, name DESC
            ) AS rn
        FROM merge_data
    )

    SELECT
        {{ dbt_utils.generate_surrogate_key(['person_source_id']) }} AS person_sur_id, 
        person_source_id,
        name,
        gender,
        profile_path,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS updated_at 
    FROM deduped_persons
    WHERE rn = 1

{% else %}

    source_daily AS (
        SELECT
            md.movie_source_id,
            md.credits
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
    ),

    casts_daily AS (
        SELECT
            c.id AS person_source_id,
            c.name AS name,
            c.gender AS gender,
            c.profile_path AS profile_path
        FROM source_daily
        CROSS JOIN UNNEST(credits."cast") AS t(c)
        WHERE c.id IS NOT NULL
    ),

    crews_daily AS (
        SELECT
            cr.id AS person_source_id,
            cr.name AS name,
            cr.gender AS gender,
            cr.profile_path AS profile_path
        FROM source_daily
        CROSS JOIN UNNEST(credits.crew) AS t(cr)
        WHERE cr.job = 'Director' AND cr.id IS NOT NULL
    ),

    merge_data AS (
        SELECT * FROM casts_daily
        UNION ALL
        SELECT * FROM crews_daily        
    ),

    deduped_persons AS (
        SELECT
            person_source_id,
            name,
            gender,
            profile_path,
            ROW_NUMBER() OVER (
                PARTITION BY person_source_id 
                ORDER BY profile_path NULLS LAST, name DESC
            ) AS rn
        FROM merge_data
    )

    SELECT
        {{ dbt_utils.generate_surrogate_key(['person_source_id']) }} AS person_sur_id,
        person_source_id,
        name,
        gender,
        profile_path,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS updated_at 
    FROM deduped_persons
    WHERE rn = 1

{% endif %}