WITH year_keyword_pairs AS (
    SELECT 
        CAST(EXTRACT(YEAR FROM d.full_date) AS VARCHAR) AS release_year,
        bk.keyword_sur_id
    FROM {{ ref('bridge_movie_keyword') }} bk
    JOIN {{ ref('dim_movies') }} m 
        ON bk.movie_sur_id = m.movie_sur_id
    LEFT JOIN {{ ref('dim_dates') }} d
        ON d.date_sur_id = m.release_date_sur_id
    WHERE m.release_date_sur_id IS NOT NULL
      AND CAST(m.release_date_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND d.full_date IS NOT NULL
      AND bk.movie_sur_id IS NOT NULL
      AND CAST(bk.movie_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bk.keyword_sur_id IS NOT NULL
      AND CAST(bk.keyword_sur_id AS VARCHAR) NOT IN ('-1', '-2')
),

term_year_freq AS (
    SELECT 
        release_year,
        keyword_sur_id,
        COUNT(*) AS term_count
    FROM year_keyword_pairs
    GROUP BY release_year, keyword_sur_id
),

year_totals AS (
    SELECT 
        release_year,
        keyword_sur_id,
        term_count,
        (term_count * 1.0) / SUM(term_count) OVER (PARTITION BY release_year) AS tf
    FROM term_year_freq
),

year_corpus_stats AS (
    SELECT 
        yt.release_year,
        yt.keyword_sur_id,
        yt.term_count,
        yt.tf,
        COUNT(*) OVER (PARTITION BY yt.keyword_sur_id) AS doc_freq,
        (SELECT COUNT(DISTINCT release_year) FROM year_keyword_pairs) AS total_years
    FROM year_totals yt
),

tf_idf_by_year AS (
    SELECT 
        release_year,
        'ALL' AS genre_sur_id,
        'ALL' AS country_sur_id,
        keyword_sur_id,
        term_count AS total_keyword_by_group,
        ROUND(CAST(tf AS DECIMAL(10, 6)), 6) AS tf,
        doc_freq AS total_group_by_keyword,
        total_years AS total_group,
        ROUND(CAST(
            LN((1.0 + total_years) / (1.0 + doc_freq)) + 1.0 
        AS DECIMAL(10, 6)), 6) AS idf,
        ROUND(CAST(
            tf * (LN((1.0 + total_years) / (1.0 + doc_freq)) + 1.0) 
        AS DECIMAL(10, 6)), 6) AS tf_idf
    FROM year_corpus_stats
),

genre_keyword_pairs AS (
    SELECT
        bg.genre_sur_id,
        bk.keyword_sur_id
    FROM {{ ref('bridge_movie_genre') }} bg
    JOIN {{ ref('bridge_movie_keyword') }} bk 
        ON bg.movie_sur_id = bk.movie_sur_id
    WHERE bg.movie_sur_id IS NOT NULL
      AND CAST(bg.movie_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bg.genre_sur_id IS NOT NULL
      AND CAST(bg.genre_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bk.keyword_sur_id IS NOT NULL
      AND CAST(bk.keyword_sur_id AS VARCHAR) NOT IN ('-1', '-2')
),

term_genre_freq AS (
    SELECT 
        genre_sur_id,
        keyword_sur_id,
        COUNT(*) AS term_count
    FROM genre_keyword_pairs
    GROUP BY genre_sur_id, keyword_sur_id
),

genre_totals AS (
    SELECT 
        genre_sur_id,
        keyword_sur_id,
        term_count,
        (term_count * 1.0) / SUM(term_count) OVER (PARTITION BY genre_sur_id) AS tf
    FROM term_genre_freq
),

genre_corpus_stats AS (
    SELECT 
        gt.genre_sur_id,
        gt.keyword_sur_id,
        gt.term_count,
        gt.tf,
        COUNT(*) OVER (PARTITION BY gt.keyword_sur_id) AS doc_freq,
        (SELECT COUNT(DISTINCT genre_sur_id) FROM genre_keyword_pairs) AS total_genres
    FROM genre_totals gt
),

tf_idf_by_genre AS (
    SELECT
        'ALL' AS release_year,
        CAST(genre_sur_id AS VARCHAR) AS genre_sur_id,
        'ALL' AS country_sur_id,
        keyword_sur_id,
        term_count AS total_keyword_by_group,
        ROUND(CAST(tf AS DECIMAL(10, 6)), 6) AS tf,
        doc_freq AS total_group_by_keyword,
        total_genres AS total_group,
        ROUND(CAST(
            LN((1.0 + total_genres) / (1.0 + doc_freq)) + 1.0 
        AS DECIMAL(10, 6)), 6) AS idf,
        ROUND(CAST(
            tf * (LN((1.0 + total_genres) / (1.0 + doc_freq)) + 1.0) 
        AS DECIMAL(10, 6)), 6) AS tf_idf
    FROM genre_corpus_stats
),

country_keyword_pairs AS (
    SELECT 
        bc.country_sur_id,
        bk.keyword_sur_id
    FROM {{ ref('bridge_movie_country') }} bc
    JOIN {{ ref('bridge_movie_keyword') }} bk 
        ON bc.movie_sur_id = bk.movie_sur_id
    WHERE bc.movie_sur_id IS NOT NULL
      AND CAST(bc.movie_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bc.country_sur_id IS NOT NULL 
      AND CAST(bc.country_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bk.keyword_sur_id IS NOT NULL
      AND CAST(bk.keyword_sur_id AS VARCHAR) NOT IN ('-1', '-2')
),

term_country_freq AS (
    SELECT 
        country_sur_id,
        keyword_sur_id,
        COUNT(*) AS term_count
    FROM country_keyword_pairs
    GROUP BY country_sur_id, keyword_sur_id
),

country_totals AS (
    SELECT 
        country_sur_id,
        keyword_sur_id,
        term_count,
        (term_count * 1.0) / SUM(term_count) OVER (PARTITION BY country_sur_id) AS tf
    FROM term_country_freq
),

country_corpus_stats AS (
    SELECT 
        ct.country_sur_id,
        ct.keyword_sur_id,
        ct.term_count,
        ct.tf,
        COUNT(*) OVER (PARTITION BY ct.keyword_sur_id) AS doc_freq,
        (SELECT COUNT(DISTINCT country_sur_id) FROM country_keyword_pairs) AS total_countries
    FROM country_totals ct
),

tf_idf_by_country AS (
    SELECT
        'ALL' AS release_year,
        'ALL' AS genre_sur_id, 
        CAST(country_sur_id AS VARCHAR) AS country_sur_id,
        keyword_sur_id,
        term_count AS total_keyword_by_group,
        ROUND(CAST(tf AS DECIMAL(10, 6)), 6) AS tf,
        doc_freq AS total_group_by_keyword,
        total_countries AS total_group,
        ROUND(CAST(
            LN((1.0 + total_countries) / (1.0 + doc_freq)) + 1.0 
        AS DECIMAL(10, 6)), 6) AS idf,
        ROUND(CAST(
            tf * (LN((1.0 + total_countries) / (1.0 + doc_freq)) + 1.0) 
        AS DECIMAL(10, 6)), 6) AS tf_idf
    FROM country_corpus_stats
),

year_genre_keyword_pairs AS (
    SELECT 
        CAST(EXTRACT(YEAR FROM d.full_date) AS VARCHAR) AS release_year,
        bg.genre_sur_id,
        bk.keyword_sur_id
    FROM {{ ref('bridge_movie_keyword') }} bk
    JOIN {{ ref('dim_movies') }} m 
        ON bk.movie_sur_id = m.movie_sur_id
    JOIN {{ ref('bridge_movie_genre') }} bg 
        ON bk.movie_sur_id = bg.movie_sur_id
    LEFT JOIN {{ ref('dim_dates') }} d 
        ON d.date_sur_id = m.release_date_sur_id
    WHERE m.release_date_sur_id IS NOT NULL
      AND CAST(m.release_date_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND d.full_date IS NOT NULL
      AND bk.movie_sur_id IS NOT NULL
      AND CAST(bk.movie_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bg.genre_sur_id IS NOT NULL
      AND CAST(bg.genre_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bk.keyword_sur_id IS NOT NULL
      AND CAST(bk.keyword_sur_id AS VARCHAR) NOT IN ('-1', '-2')
),

term_year_genre_freq AS (
    SELECT 
        release_year,
        genre_sur_id,
        keyword_sur_id,
        COUNT(*) AS term_count
    FROM year_genre_keyword_pairs
    GROUP BY release_year, genre_sur_id, keyword_sur_id
),

year_genre_totals AS (
    SELECT 
        release_year,
        genre_sur_id,
        keyword_sur_id,
        term_count,
        (term_count * 1.0) / SUM(term_count) OVER (PARTITION BY release_year, genre_sur_id) AS tf
    FROM term_year_genre_freq
),

year_genre_corpus_stats AS (
    SELECT 
        ygt.release_year,
        ygt.genre_sur_id,
        ygt.keyword_sur_id,
        ygt.term_count,
        ygt.tf,
        COUNT(*) OVER (PARTITION BY ygt.keyword_sur_id) AS doc_freq,
        (SELECT COUNT(DISTINCT CONCAT(release_year, '_', CAST(genre_sur_id AS VARCHAR))) 
         FROM year_genre_keyword_pairs) AS total_year_genres
    FROM year_genre_totals ygt
),

tf_idf_by_year_genre AS (
    SELECT 
        release_year,
        CAST(genre_sur_id AS VARCHAR) AS genre_sur_id,
        'ALL' AS country_sur_id,
        keyword_sur_id,
        term_count AS total_keyword_by_group,
        ROUND(CAST(tf AS DECIMAL(10, 6)), 6) AS tf,
        doc_freq AS total_group_by_keyword,
        total_year_genres AS total_group,
        ROUND(CAST(
            LN((1.0 + total_year_genres) / (1.0 + doc_freq)) + 1.0 
        AS DECIMAL(10, 6)), 6) AS idf,
        ROUND(CAST(
            tf * (LN((1.0 + total_year_genres) / (1.0 + doc_freq)) + 1.0) 
        AS DECIMAL(10, 6)), 6) AS tf_idf
    FROM year_genre_corpus_stats
),

year_country_keyword_pairs AS (
    SELECT 
        CAST(EXTRACT(YEAR FROM d.full_date) AS VARCHAR) AS release_year,
        bc.country_sur_id,
        bk.keyword_sur_id
    FROM {{ ref('bridge_movie_keyword') }} bk
    JOIN {{ ref('dim_movies') }} m 
        ON bk.movie_sur_id = m.movie_sur_id
    JOIN {{ ref('bridge_movie_country') }} bc 
        ON bk.movie_sur_id = bc.movie_sur_id
    LEFT JOIN {{ ref('dim_dates') }} d
        ON d.date_sur_id = m.release_date_sur_id
    WHERE m.release_date_sur_id IS NOT NULL
      AND CAST(m.release_date_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND d.full_date IS NOT NULL
      AND bk.movie_sur_id IS NOT NULL
      AND CAST(bk.movie_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bc.country_sur_id IS NOT NULL
      AND CAST(bc.country_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bk.keyword_sur_id IS NOT NULL
      AND CAST(bk.keyword_sur_id AS VARCHAR) NOT IN ('-1', '-2')
),

term_year_country_freq AS (
    SELECT 
        release_year,
        country_sur_id,
        keyword_sur_id,
        COUNT(*) AS term_count
    FROM year_country_keyword_pairs
    GROUP BY release_year, country_sur_id, keyword_sur_id
),

year_country_totals AS (
    SELECT 
        release_year,
        country_sur_id,
        keyword_sur_id,
        term_count,
        (term_count * 1.0) / SUM(term_count) OVER (PARTITION BY release_year, country_sur_id) AS tf
    FROM term_year_country_freq
),

year_country_corpus_stats AS (
    SELECT 
        yct.release_year,
        yct.country_sur_id,
        yct.keyword_sur_id,
        yct.term_count,
        yct.tf,
        COUNT(*) OVER (PARTITION BY yct.keyword_sur_id) AS doc_freq,
        (SELECT COUNT(DISTINCT CONCAT(release_year, '_', CAST(country_sur_id AS VARCHAR))) 
         FROM year_country_keyword_pairs) AS total_year_countries
    FROM year_country_totals yct
),

tf_idf_by_year_country AS (
    SELECT 
        release_year,
        'ALL' AS genre_sur_id,
        CAST(country_sur_id AS VARCHAR) AS country_sur_id,
        keyword_sur_id,
        term_count AS total_keyword_by_group,
        ROUND(CAST(tf AS DECIMAL(10, 6)), 6) AS tf,
        doc_freq AS total_group_by_keyword,
        total_year_countries AS total_group,
        ROUND(CAST(
            LN((1.0 + total_year_countries) / (1.0 + doc_freq)) + 1.0 
        AS DECIMAL(10, 6)), 6) AS idf,
        ROUND(CAST(
            tf * (LN((1.0 + total_year_countries) / (1.0 + doc_freq)) + 1.0) 
        AS DECIMAL(10, 6)), 6) AS tf_idf
    FROM year_country_corpus_stats
),

genre_country_keyword_pairs AS (
    SELECT 
        bg.genre_sur_id,
        bc.country_sur_id,
        bk.keyword_sur_id
    FROM {{ ref('bridge_movie_keyword') }} bk
    JOIN {{ ref('bridge_movie_genre') }} bg 
        ON bk.movie_sur_id = bg.movie_sur_id
    JOIN {{ ref('bridge_movie_country') }} bc 
        ON bk.movie_sur_id = bc.movie_sur_id
    WHERE bk.movie_sur_id IS NOT NULL
      AND CAST(bk.movie_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bg.genre_sur_id IS NOT NULL
      AND CAST(bg.genre_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bc.country_sur_id IS NOT NULL
      AND CAST(bc.country_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bk.keyword_sur_id IS NOT NULL
      AND CAST(bk.keyword_sur_id AS VARCHAR) NOT IN ('-1', '-2')
),

term_genre_country_freq AS (
    SELECT 
        genre_sur_id,
        country_sur_id,
        keyword_sur_id,
        COUNT(*) AS term_count
    FROM genre_country_keyword_pairs
    GROUP BY genre_sur_id, country_sur_id, keyword_sur_id
),

genre_country_totals AS (
    SELECT 
        genre_sur_id,
        country_sur_id,
        keyword_sur_id,
        term_count,
        (term_count * 1.0) / SUM(term_count) OVER (PARTITION BY genre_sur_id, country_sur_id) AS tf
    FROM term_genre_country_freq
),

genre_country_corpus_stats AS (
    SELECT 
        gct.genre_sur_id,
        gct.country_sur_id,
        gct.keyword_sur_id,
        gct.term_count,
        gct.tf,
        COUNT(*) OVER (PARTITION BY gct.keyword_sur_id) AS doc_freq,
        (SELECT COUNT(DISTINCT CONCAT(CAST(genre_sur_id AS VARCHAR), '_', CAST(country_sur_id AS VARCHAR))) 
         FROM genre_country_keyword_pairs) AS total_genre_countries
    FROM genre_country_totals gct
),

tf_idf_by_genre_country AS (
    SELECT 
        'ALL' AS release_year, 
        CAST(genre_sur_id AS VARCHAR) AS genre_sur_id,
        CAST(country_sur_id AS VARCHAR) AS country_sur_id,
        keyword_sur_id,
        term_count AS total_keyword_by_group,
        ROUND(CAST(tf AS DECIMAL(10, 6)), 6) AS tf,
        doc_freq AS total_group_by_keyword,
        total_genre_countries AS total_group,
        ROUND(CAST(
            LN((1.0 + total_genre_countries) / (1.0 + doc_freq)) + 1.0 
        AS DECIMAL(10, 6)), 6) AS idf,
        ROUND(CAST(
            tf * (LN((1.0 + total_genre_countries) / (1.0 + doc_freq)) + 1.0) 
        AS DECIMAL(10, 6)), 6) AS tf_idf
    FROM genre_country_corpus_stats
),

year_genre_country_keyword_pairs AS (
    SELECT 
        CAST(EXTRACT(YEAR FROM d.full_date) AS VARCHAR) AS release_year,
        bg.genre_sur_id,
        bc.country_sur_id,
        bk.keyword_sur_id
    FROM {{ ref('bridge_movie_keyword') }} bk
    JOIN {{ ref('dim_movies') }} m 
        ON bk.movie_sur_id = m.movie_sur_id
    JOIN {{ ref('bridge_movie_genre') }} bg 
        ON bk.movie_sur_id = bg.movie_sur_id
    JOIN {{ ref('bridge_movie_country') }} bc 
        ON bk.movie_sur_id = bc.movie_sur_id
    LEFT JOIN {{ ref('dim_dates') }} d
        ON d.date_sur_id = m.release_date_sur_id
    WHERE m.release_date_sur_id IS NOT NULL
      AND CAST(m.release_date_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND d.full_date IS NOT NULL
      AND bk.movie_sur_id IS NOT NULL
      AND CAST(bk.movie_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bg.genre_sur_id IS NOT NULL
      AND CAST(bg.genre_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bc.country_sur_id IS NOT NULL
      AND CAST(bc.country_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND bk.keyword_sur_id IS NOT NULL
      AND CAST(bk.keyword_sur_id AS VARCHAR) NOT IN ('-1', '-2')
),

term_year_genre_country_freq AS (
    SELECT 
        release_year,
        genre_sur_id,
        country_sur_id,
        keyword_sur_id,
        COUNT(*) AS term_count
    FROM year_genre_country_keyword_pairs
    GROUP BY release_year, genre_sur_id, country_sur_id, keyword_sur_id
),

year_genre_country_totals AS (
    SELECT 
        release_year,
        genre_sur_id,
        country_sur_id,
        keyword_sur_id,
        term_count,
        (term_count * 1.0) / SUM(term_count) OVER (PARTITION BY release_year, genre_sur_id, country_sur_id) AS tf
    FROM term_year_genre_country_freq
),

year_genre_country_corpus_stats AS (
    SELECT 
        ygct.release_year,
        ygct.genre_sur_id,
        ygct.country_sur_id,
        ygct.keyword_sur_id,
        ygct.term_count,
        ygct.tf,
        COUNT(*) OVER (PARTITION BY ygct.keyword_sur_id) AS doc_freq,
        (SELECT COUNT(DISTINCT CONCAT(release_year, '_', CAST(genre_sur_id AS VARCHAR), '_', CAST(country_sur_id AS VARCHAR))) 
         FROM year_genre_country_keyword_pairs) AS total_year_genre_countries
    FROM year_genre_country_totals ygct
),

tf_idf_by_year_genre_country AS (
    SELECT 
        release_year,
        CAST(genre_sur_id AS VARCHAR) AS genre_sur_id,
        CAST(country_sur_id AS VARCHAR) AS country_sur_id,
        keyword_sur_id,
        term_count AS total_keyword_by_group,
        ROUND(CAST(tf AS DECIMAL(10, 6)), 6) AS tf,
        doc_freq AS total_group_by_keyword,
        total_year_genre_countries AS total_group, 
        ROUND(CAST(
            LN((1.0 + total_year_genre_countries) / (1.0 + doc_freq)) + 1.0 
        AS DECIMAL(10, 6)), 6) AS idf,
        ROUND(CAST(
            tf * (LN((1.0 + total_year_genre_countries) / (1.0 + doc_freq)) + 1.0) 
        AS DECIMAL(10, 6)), 6) AS tf_idf
    FROM year_genre_country_corpus_stats
),

global_keyword_pairs AS (
    SELECT 
        movie_sur_id,
        keyword_sur_id
    FROM {{ ref('bridge_movie_keyword') }}
    WHERE movie_sur_id IS NOT NULL 
      AND CAST(movie_sur_id AS VARCHAR) NOT IN ('-1', '-2')
      AND keyword_sur_id IS NOT NULL
      AND CAST(keyword_sur_id AS VARCHAR) NOT IN ('-1', '-2')
),

term_global_freq AS (
    SELECT 
        keyword_sur_id,
        COUNT(*) AS term_count,
        COUNT(DISTINCT movie_sur_id) AS doc_freq
    FROM global_keyword_pairs
    GROUP BY keyword_sur_id
),

global_totals AS (
    SELECT 
        keyword_sur_id,
        term_count,
        doc_freq,
        (term_count * 1.0) / SUM(term_count) OVER () AS tf
    FROM term_global_freq
),

global_corpus_stats AS (
    SELECT 
        gt.keyword_sur_id,
        gt.term_count,
        gt.doc_freq,
        gt.tf,
        (SELECT COUNT(DISTINCT movie_sur_id) FROM global_keyword_pairs) AS total_movies
    FROM global_totals gt
),

tf_idf_global AS (
    SELECT
        'ALL' AS release_year,
        'ALL' AS genre_sur_id,
        'ALL' AS country_sur_id, 
        keyword_sur_id,
        term_count AS total_keyword_by_group,
        ROUND(CAST(tf AS DECIMAL(10, 6)), 6) AS tf,
        doc_freq AS total_group_by_keyword,
        total_movies AS total_group,
        ROUND(CAST(
            LN((1.0 + total_movies) / (1.0 + doc_freq)) + 1.0 
        AS DECIMAL(10, 6)), 6) AS idf,
        ROUND(CAST(
            tf * (LN((1.0 + total_movies) / (1.0 + doc_freq)) + 1.0) 
        AS DECIMAL(10, 6)), 6) AS tf_idf
    FROM global_corpus_stats
),

all_level AS (
    SELECT * FROM tf_idf_by_year
    UNION ALL
    SELECT * FROM tf_idf_by_genre
    UNION ALL
    SELECT * FROM tf_idf_by_country
    UNION ALL
    SELECT * FROM tf_idf_by_year_genre
    UNION ALL
    SELECT * FROM tf_idf_by_year_country
    UNION ALL
    SELECT * FROM tf_idf_by_genre_country
    UNION ALL
    SELECT * FROM tf_idf_by_year_genre_country
    UNION ALL
    SELECT * FROM tf_idf_global
)

SELECT 
    a.release_year,
    k.keyword,
    a.total_keyword_by_group,
    a.tf,
    a.total_group_by_keyword,
    a.total_group,
    a.idf,
    a.tf_idf,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS calculated_at,
    CASE 
        WHEN a.country_sur_id = 'ALL' THEN 'ALL' 
        ELSE c.country_name 
    END AS country_name,
        CASE 
        WHEN a.genre_sur_id = 'ALL' THEN 'ALL' 
        ELSE g.genre 
    END AS genre
FROM all_level a
LEFT JOIN {{ ref('dim_genres') }} g
    ON a.genre_sur_id = CAST(g.genre_sur_id AS VARCHAR)
    AND a.genre_sur_id != 'ALL'
LEFT JOIN {{ ref('dim_countries') }} c
    ON a.country_sur_id = CAST(c.country_sur_id AS VARCHAR)
    AND a.country_sur_id != 'ALL'
LEFT JOIN {{ ref('dim_keywords') }} k
    ON a.keyword_sur_id = k.keyword_sur_id