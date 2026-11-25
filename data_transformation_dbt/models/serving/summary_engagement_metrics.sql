WITH base_engagement AS (
    SELECT
        f.movie_sur_id,
        f.days_since_release,
        f.vote_average,
        f.vote_count,
        f.popularity
    FROM {{ ref('fact_movie_daily_engagement_30d') }} f
    INNER JOIN {{ ref('dim_movies') }} m
        ON f.movie_sur_id = m.movie_sur_id
    WHERE CAST(f.movie_sur_id AS VARCHAR) NOT IN ('-1', '-2') -- ตัดหนังที่เป็น -1, -2
      AND m.status = 'Released'
      AND f.days_since_release IS NOT NULL
),

-- =========================================================================
-- ส่วนที่ A: genre = 'ALL' (รวมทุกประเภทหนัง แบ่งตาม days_since_release)
-- =========================================================================
all_genre_engagement_agg AS (
    SELECT
        'ALL' AS genre,
        b.days_since_release,
        COUNT(DISTINCT b.movie_sur_id) AS total_movies,
        ROUND(AVG(b.vote_count), 2) AS avg_vote_count,
        ROUND(AVG(b.vote_average), 2) AS avg_vote_average,
        ROUND(AVG(b.popularity), 3) AS avg_popularity
    FROM base_engagement b
    GROUP BY
        b.days_since_release
),

-- =========================================================================
-- ส่วนที่ B: genre != 'ALL' (แบ่งตาม genre และ days_since_release)
-- =========================================================================
genre_engagement_agg AS (
    SELECT
        g.genre,
        b.days_since_release,
        COUNT(DISTINCT b.movie_sur_id) AS total_movies,
        ROUND(AVG(b.vote_count), 2) AS avg_vote_count,
        ROUND(AVG(b.vote_average), 2) AS avg_vote_average,
        ROUND(AVG(b.popularity), 3) AS avg_popularity
    FROM base_engagement b
    INNER JOIN {{ ref('bridge_movie_genre') }} bg
        ON b.movie_sur_id = bg.movie_sur_id
        AND CAST(bg.genre_sur_id AS VARCHAR) NOT IN ('-1', '-2')
    INNER JOIN {{ ref('dim_genres') }} g
        ON bg.genre_sur_id = g.genre_sur_id
    GROUP BY
        g.genre,
        b.days_since_release
)

SELECT
    days_since_release,
    total_movies,
    avg_vote_count,
    avg_vote_average,
    avg_popularity,
    genre
FROM (
    SELECT * FROM all_genre_engagement_agg
    UNION ALL
    SELECT * FROM genre_engagement_agg
)
ORDER BY
    CASE WHEN genre = 'ALL' THEN 0 ELSE 1 END,
    genre,
    days_since_release