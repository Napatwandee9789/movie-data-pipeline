{{
    config(
        materialized = 'incremental',
        unique_key = 'movie_source_id',
        incremental_strategy = 'merge',
        merge_update_columns = [
            'budget', 'revenue', 'profit', 'roi', 'revenue_multiplier', 
            'runtime', 'status', 'original_language', 'original_title', 
            'title', 'overview', 'poster_path', 'backdrop_path', 
            'release_date_sur_id', 'updated_at'
        ]
    )
}}

WITH movies_raw AS (

    {% if not is_incremental() %} -- Case 1: Full Refresh

    SELECT 
        movie_source_id,
        budget,
        revenue,
        profit,
        roi,
        revenue_multiplier,
        runtime,
        status,
        original_language,
        original_title,
        title,
        overview,
        poster_path,
        backdrop_path,
        release_date,
        -- ให้ historical เป็น partition ค่าต่ำสุดเพื่อยอมให้ daily ชนะเสมอ
        '1900' AS partition_year,
        '01' AS partition_month,
        '01' AS partition_day
    FROM {{ ref('int_movie_historical') }}

    UNION ALL
    
    SELECT 
        movie_source_id,
        budget,
        revenue,
        profit,
        roi,
        revenue_multiplier,
        runtime,
        status,
        original_language,
        original_title,
        title,
        overview,
        poster_path,
        backdrop_path,
        release_date,
        CAST(year AS VARCHAR) AS partition_year,
        CAST(month AS VARCHAR) AS partition_month,
        CAST(day AS VARCHAR) AS partition_day
    FROM {{ ref('int_movie_daily_changes') }}

    {% else %} -- Case 2: Incremental Load

    SELECT 
        movie_source_id,
        budget,
        revenue,
        profit,
        roi,
        revenue_multiplier,
        runtime,
        status,
        original_language,
        original_title,
        title,
        overview,
        poster_path,
        backdrop_path,
        release_date,
        CAST(year AS VARCHAR) AS partition_year,
        CAST(month AS VARCHAR) AS partition_month,
        CAST(day AS VARCHAR) AS partition_day
    FROM {{ ref('int_movie_daily_changes') }}
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
    {% endif %}
),

movies_deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY movie_source_id 
            ORDER BY 
                partition_year DESC, 
                partition_month DESC, 
                partition_day DESC
        ) AS rn
    FROM movies_raw
    WHERE movie_source_id IS NOT NULL
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['movie_source_id']) }} AS movie_sur_id,
    movie_source_id,
    budget,
    revenue,
    profit,
    roi,
    revenue_multiplier,
    runtime,
    status,
    original_language,
    original_title,
    title,
    overview,
    poster_path,
    backdrop_path,
    COALESCE(CAST(DATE_FORMAT(CAST(release_date AS TIMESTAMP), '%Y%m%d') AS BIGINT), -1) AS release_date_sur_id,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS updated_at
FROM movies_deduped
WHERE rn = 1