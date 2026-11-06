{{
    config(
        materialized = 'incremental',
        unique_key = 'movie_person_bridge_id',
        incremental_strategy = 'merge',
        merge_update_columns = []
    )
}}

WITH 
{% if not is_incremental() %} -- Case 1: Full Refresh

    source_historical AS (
        SELECT
            mh.movie_source_id,
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

    raw_casts_historical AS (
        SELECT
            s.movie_source_id,
            c.id AS person_id,
            'Actor' AS role
        FROM source_historical s
        CROSS JOIN UNNEST(s.credits."cast") AS t(c)
        INNER JOIN {{ref('dim_persons')}} dp
        ON c.id = dp.person_source_id
    ),

    movies_casts_historical AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['movie_source_id', 'person_id', "'Actor'"]) }} AS movie_person_bridge_id,
            {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id,
            {{ dbt_utils.generate_surrogate_key(['person_id']) }} AS person_sur_id,
            role
        FROM raw_casts_historical
    ),

    raw_casts_daily AS (
        SELECT 
            s.movie_source_id,
            c.id AS person_id,
            'Actor' AS role
        FROM source_daily s
        CROSS JOIN UNNEST(s.credits."cast") AS t(c)
        INNER JOIN {{ref('dim_persons')}} dp
        ON c.id = dp.person_source_id
    ),

    movies_casts_daily AS (
        SELECT 
            {{ dbt_utils.generate_surrogate_key(['movie_source_id', 'person_id', "'Actor'"]) }} AS movie_person_bridge_id,
            {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id,
            {{ dbt_utils.generate_surrogate_key(['person_id']) }} AS person_sur_id,
            role
        FROM raw_casts_daily
    ),

    raw_crews_historical AS (
        SELECT
            s.movie_source_id,
            cr.id AS person_id,
            cr.job AS role
        FROM source_historical s
        CROSS JOIN UNNEST(s.credits.crew) AS t(cr)
        INNER JOIN {{ref('dim_persons')}} dp
        ON cr.id = dp.person_source_id  
        WHERE cr.job = 'Director'                 
    ),

    movies_crews_historical AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['movie_source_id', 'person_id', 'role']) }} AS movie_person_bridge_id,
            {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id,
            {{ dbt_utils.generate_surrogate_key(['person_id']) }} AS person_sur_id,
            role
        FROM raw_crews_historical
    ),

    raw_crews_daily AS (
        SELECT
            s.movie_source_id,
            cr.id AS person_id,
            cr.job AS role
        FROM source_daily s
        CROSS JOIN UNNEST(s.credits.crew) AS t(cr)
        INNER JOIN {{ref('dim_persons')}} dp
        ON cr.id = dp.person_source_id  
        WHERE cr.job = 'Director'                
    ),

    movies_crews_daily AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['movie_source_id', 'person_id', 'role']) }} AS movie_person_bridge_id,
            {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id,
            {{ dbt_utils.generate_surrogate_key(['person_id']) }} AS person_sur_id,
            role
        FROM raw_crews_daily
    ),

    merge_data AS (
        SELECT * FROM movies_casts_historical
        UNION 
        SELECT * FROM movies_casts_daily
        UNION
        SELECT * FROM movies_crews_historical
        UNION
        SELECT * FROM movies_crews_daily
    )

    SELECT 
        *,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at 
    FROM merge_data

{% else %} -- Case 2: Incremental Load

    source_daily AS (
        SELECT
            dm.movie_source_id AS movie_source_id,
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

    raw_casts_daily AS (
        SELECT
            s.movie_source_id,
            c.id AS person_id,
            'Actor' AS role
        FROM source_daily s
        CROSS JOIN UNNEST(s.credits."cast") AS t(c)
        INNER JOIN {{ref('dim_persons')}} dp
        ON c.id = dp.person_source_id
    ),

    casts_daily AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['movie_source_id', 'person_id', "'Actor'"]) }} AS movie_person_bridge_id,
            {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id,
            {{ dbt_utils.generate_surrogate_key(['person_id']) }} AS person_sur_id,
            role
        FROM raw_casts_daily
    ),

    raw_crews_daily AS (
        SELECT 
            s.movie_source_id,
            cr.id AS person_id,
            cr.job AS role
        FROM source_daily s
        CROSS JOIN UNNEST(s.credits.crew) AS t(cr)  
        INNER JOIN {{ref('dim_persons')}} dp
        ON cr.id = dp.person_source_id
        WHERE cr.job = 'Director'                
    ),

    crews_daily AS (
        SELECT 
            {{ dbt_utils.generate_surrogate_key(['movie_source_id', 'person_id', 'role']) }} AS movie_person_bridge_id,
            {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id,
            {{ dbt_utils.generate_surrogate_key(['person_id']) }} AS person_sur_id,
            role
        FROM raw_crews_daily
    ),

    merge_data AS (
        SELECT * FROM casts_daily
        UNION  
        SELECT * FROM crews_daily         
    )

    SELECT 
        *,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
    FROM merge_data

{% endif %}