{{
    config(
        materialized = 'incremental',
        unique_key = 'fact_movie_daily_id',
        incremental_strategy = 'merge',
        merge_update_columns = [],
        partitioned_by=['days_since_release']
        )
}}
WITH

{% if not is_incremental() %} -- Case 1: Full Refresh

 prepared_data AS
    (
        SELECT 
        {{dbt_utils.generate_surrogate_key(['mds.movie_source_id',])}} as movie_sur_id,
        mds.vote_average,
        mds.vote_count,
        mds.popularity,
        days_since_release,
        CAST(CONCAT(mds.year, mds.month, mds.day) AS BIGINT) AS collected_date_sur_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
        FROM {{ref('stg_movie_daily_snapshots')}} mds
        INNER JOIN {{ref('dim_movies')}} dm
        ON mds.movie_source_id = dm.movie_source_id
    )

{% else %} -- Case 2: Incremental Load

    prepared_data AS
        (
            SELECT 
            {{dbt_utils.generate_surrogate_key(['mds.movie_source_id',])}} as movie_sur_id,
            mds.vote_average,
            mds.vote_count,
            mds.popularity,
            days_since_release,
            CAST(CONCAT(mds.year, mds.month, mds.day) AS BIGINT) AS collected_date_sur_id,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS loaded_at
            FROM {{ref('stg_movie_daily_snapshots')}} mds
            INNER JOIN {{ref('dim_movies')}} dm
            ON mds.movie_source_id = dm.movie_source_id
            WHERE
                (year, month, day) IN (
                    {% for i in range(0, 1) %}
                        (
                            '{{ (run_started_at - modules.datetime.timedelta(days=i)).strftime("%Y") }}',
                            '{{ (run_started_at - modules.datetime.timedelta(days=i)).strftime("%m") }}',
                            '{{ (run_started_at - modules.datetime.timedelta(days=i)).strftime("%d") }}'
                        ){{ "," if not loop.last }})
                    {% endfor %}
        )
{% endif %}


SELECT 
    *,
    {{dbt_utils.generate_surrogate_key(['movie_sur_id', 'collected_date_sur_id'])}} as fact_movie_daily_id
FROM prepared_data

