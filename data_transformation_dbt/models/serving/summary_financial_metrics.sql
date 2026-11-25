WITH base_movies AS (
    SELECT
        m.movie_sur_id,
        m.title,
        d.year AS release_year,
        d.quarter AS release_quarter,
        d.month AS release_month,
        m.budget,
        m.revenue,
        m.profit,
        m.roi,
        m.revenue_multiplier,
        m.runtime
    FROM "awsdatacatalog"."marts"."dim_movies" m
    LEFT JOIN "awsdatacatalog"."marts"."dim_dates" d 
        ON CAST(m.release_date_sur_id AS BIGINT) = d.date_sur_id
        AND d.date_sur_id NOT IN (-1, -2) -- ตัดวันฉายที่เป็น -1, -2
    WHERE CAST(m.movie_sur_id AS VARCHAR) NOT IN ('-1', '-2') -- ตัดหนังที่เป็น -1, -2
      AND m.release_date_sur_id NOT IN (-1, -2)
      AND m.status = 'Released'
      AND m.budget IS NOT NULL AND m.budget > 0
      AND m.revenue IS NOT NULL AND m.revenue > 0
),

-- =========================================================================
-- Macro สร้าง JSON Distribution ที่มีชื่อ Key ตรงตามสเปก
-- =========================================================================


-- =========================================================================
-- ส่วนที่ A: genre = 'ALL' 
-- =========================================================================

all_genre_time_agg AS (
    SELECT
        'ALL' AS genre,
        COALESCE(CAST(b.release_year AS VARCHAR), 'ALL') AS release_year,
        COALESCE(CAST(b.release_quarter AS VARCHAR), 'ALL') AS release_quarter,
        COALESCE(CAST(b.release_month AS VARCHAR), 'ALL') AS release_month,
        'ALL' AS iso_3166_1,
        'ALL' AS country_name,
        COUNT(b.movie_sur_id) AS total_movies,
        ROUND(AVG(b.budget), 2) AS avg_budget,
        ROUND(AVG(b.revenue), 2) AS avg_revenue,
        ROUND(AVG(b.profit), 2) AS avg_profit,
        ROUND(AVG(b.roi), 4) AS avg_roi,
        ROUND(AVG(b.revenue_multiplier), 4) AS avg_revenue_multiplier,
        ROUND(AVG(b.runtime), 2) AS avg_runtime,
        
json_format(CAST(MAP(
    ARRAY['budget', 'revenue', 'runtime', 'profit', 'roi', 'revenue_multiplier'],
    ARRAY[
        -- 1. BUDGET
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.budget, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.budget, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.budget, 0.75) AS JSON),
                CAST(MAX(b.budget) AS JSON),
                CAST(MAX_BY(b.title, b.budget) AS JSON),
                CAST(MAP(
                    ARRAY['0', '<1M', '1M-10M', '10M-50M', '50M-100M', '100M-200M', '>200M'],
                    ARRAY[
                        COUNT_IF(b.budget = 0),
                        COUNT_IF(b.budget > 0 AND b.budget < 1000000),
                        COUNT_IF(b.budget >= 1000000 AND b.budget < 10000000),
                        COUNT_IF(b.budget >= 10000000 AND b.budget < 50000000),
                        COUNT_IF(b.budget >= 50000000 AND b.budget < 100000000),
                        COUNT_IF(b.budget >= 100000000 AND b.budget < 200000000),
                        COUNT_IF(b.budget >= 200000000)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 2. REVENUE
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.revenue, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue, 0.75) AS JSON),
                CAST(MAX(b.revenue) AS JSON),
                CAST(MAX_BY(b.title, b.revenue) AS JSON),
                CAST(MAP(
                    ARRAY['0', '<1M', '1M-10M', '10M-50M', '50M-100M', '100M-500M', '>500M'],
                    ARRAY[
                        COUNT_IF(b.revenue = 0),
                        COUNT_IF(b.revenue > 0 AND b.revenue < 1000000),
                        COUNT_IF(b.revenue >= 1000000 AND b.revenue < 10000000),
                        COUNT_IF(b.revenue >= 10000000 AND b.revenue < 50000000),
                        COUNT_IF(b.revenue >= 50000000 AND b.revenue < 100000000),
                        COUNT_IF(b.revenue >= 100000000 AND b.revenue < 500000000),
                        COUNT_IF(b.revenue >= 500000000)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 3. RUNTIME (หน่วย: นาที)
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.runtime, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.runtime, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.runtime, 0.75) AS JSON),
                CAST(MAX(b.runtime) AS JSON),
                CAST(MAX_BY(b.title, b.runtime) AS JSON),
                CAST(MAP(
                    ARRAY['0', '<60m', '60-90m', '90-120m', '120-150m', '150-180m', '>180m'],
                    ARRAY[
                        COUNT_IF(b.runtime = 0),
                        COUNT_IF(b.runtime > 0 AND b.runtime < 60),
                        COUNT_IF(b.runtime >= 60 AND b.runtime < 90),
                        COUNT_IF(b.runtime >= 90 AND b.runtime < 120),
                        COUNT_IF(b.runtime >= 120 AND b.runtime < 150),
                        COUNT_IF(b.runtime >= 150 AND b.runtime < 180),
                        COUNT_IF(b.runtime >= 180)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 4. PROFIT
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.profit, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.profit, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.profit, 0.75) AS JSON),
                CAST(MAX(b.profit) AS JSON),
                CAST(MAX_BY(b.title, b.profit) AS JSON),
                CAST(MAP(
                    ARRAY['<0 (ขาดทุน)', '0', '0-10M', '10M-50M', '50M-100M', '100M-500M', '>500M'],
                    ARRAY[
                        COUNT_IF(b.profit < 0),
                        COUNT_IF(b.profit = 0),
                        COUNT_IF(b.profit > 0 AND b.profit < 10000000),
                        COUNT_IF(b.profit >= 10000000 AND b.profit < 50000000),
                        COUNT_IF(b.profit >= 50000000 AND b.profit < 100000000),
                        COUNT_IF(b.profit >= 100000000 AND b.profit < 500000000),
                        COUNT_IF(b.profit >= 500000000)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 5. ROI
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.roi, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.roi, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.roi, 0.75) AS JSON),
                CAST(MAX(b.roi) AS JSON),
                CAST(MAX_BY(b.title, b.roi) AS JSON),
                CAST(MAP(
                    ARRAY['<0%', '0%', '0-50%', '50-100%', '100-300%', '300-1000%', '>1000%'],
                    ARRAY[
                        COUNT_IF(b.roi < 0),
                        COUNT_IF(b.roi = 0),
                        COUNT_IF(b.roi > 0 AND b.roi < 0.5),
                        COUNT_IF(b.roi >= 0.5 AND b.roi < 1.0),
                        COUNT_IF(b.roi >= 1.0 AND b.roi < 3.0),
                        COUNT_IF(b.roi >= 3.0 AND b.roi < 10.0),
                        COUNT_IF(b.roi >= 10.0)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 6. REVENUE MULTIPLIER
        CAST(MAP(
            ARRAY[
                'p25', 'median', 'p75', 'max_val', 'top_movie',
                'profitable_count', 'unprofitable_count', 'profitable_percent', 'unprofitable_percent',
                'bins'
            ],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.revenue_multiplier, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue_multiplier, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue_multiplier, 0.75) AS JSON),
                CAST(MAX(b.revenue_multiplier) AS JSON),
                CAST(MAX_BY(b.title, b.revenue_multiplier) AS JSON),
                CAST(COUNT_IF(b.revenue_multiplier >= 2.5) AS JSON),
                CAST(COUNT_IF(b.revenue_multiplier < 2.5) AS JSON),
                CAST(ROUND(COUNT_IF(b.revenue_multiplier >= 2.5) * 100.0 / NULLIF(COUNT(b.revenue_multiplier), 0), 2) AS JSON),
                CAST(ROUND(COUNT_IF(b.revenue_multiplier < 2.5) * 100.0 / NULLIF(COUNT(b.revenue_multiplier), 0), 2) AS JSON),
                CAST(MAP(
                    ARRAY['0x', '<1x', '1x-2x', '2x-5x', '5x-10x', '>10x'],
                    ARRAY[
                        COUNT_IF(b.revenue_multiplier = 0),
                        COUNT_IF(b.revenue_multiplier > 0 AND b.revenue_multiplier < 1),
                        COUNT_IF(b.revenue_multiplier >= 1 AND b.revenue_multiplier < 2),
                        COUNT_IF(b.revenue_multiplier >= 2 AND b.revenue_multiplier < 5),
                        COUNT_IF(b.revenue_multiplier >= 5 AND b.revenue_multiplier < 10),
                        COUNT_IF(b.revenue_multiplier >= 10)
                    ]
                ) AS JSON)
            ]
        ) AS JSON)
    ]
) AS JSON))
 AS distribution_metrics
    FROM base_movies b
    GROUP BY GROUPING SETS (
        (),
        (b.release_quarter),
        (b.release_quarter, b.release_month),
        (b.release_year),
        (b.release_year, b.release_quarter),
        (b.release_year, b.release_quarter, b.release_month)
    )
),

all_genre_country_agg AS (
    SELECT
        'ALL' AS genre,
        COALESCE(CAST(b.release_year AS VARCHAR), 'ALL') AS release_year,
        COALESCE(CAST(b.release_quarter AS VARCHAR), 'ALL') AS release_quarter,
        COALESCE(CAST(b.release_month AS VARCHAR), 'ALL') AS release_month,
        c.iso_3166_1,
        c.country_name,
        COUNT(b.movie_sur_id) AS total_movies,
        ROUND(AVG(b.budget), 2) AS avg_budget,
        ROUND(AVG(b.revenue), 2) AS avg_revenue,
        ROUND(AVG(b.profit), 2) AS avg_profit,
        ROUND(AVG(b.roi), 4) AS avg_roi,
        ROUND(AVG(b.revenue_multiplier), 4) AS avg_revenue_multiplier,
        ROUND(AVG(b.runtime), 2) AS avg_runtime,
        
json_format(CAST(MAP(
    ARRAY['budget', 'revenue', 'runtime', 'profit', 'roi', 'revenue_multiplier'],
    ARRAY[
        -- 1. BUDGET
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.budget, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.budget, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.budget, 0.75) AS JSON),
                CAST(MAX(b.budget) AS JSON),
                CAST(MAX_BY(b.title, b.budget) AS JSON),
                CAST(MAP(
                    ARRAY['0', '<1M', '1M-10M', '10M-50M', '50M-100M', '100M-200M', '>200M'],
                    ARRAY[
                        COUNT_IF(b.budget = 0),
                        COUNT_IF(b.budget > 0 AND b.budget < 1000000),
                        COUNT_IF(b.budget >= 1000000 AND b.budget < 10000000),
                        COUNT_IF(b.budget >= 10000000 AND b.budget < 50000000),
                        COUNT_IF(b.budget >= 50000000 AND b.budget < 100000000),
                        COUNT_IF(b.budget >= 100000000 AND b.budget < 200000000),
                        COUNT_IF(b.budget >= 200000000)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 2. REVENUE
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.revenue, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue, 0.75) AS JSON),
                CAST(MAX(b.revenue) AS JSON),
                CAST(MAX_BY(b.title, b.revenue) AS JSON),
                CAST(MAP(
                    ARRAY['0', '<1M', '1M-10M', '10M-50M', '50M-100M', '100M-500M', '>500M'],
                    ARRAY[
                        COUNT_IF(b.revenue = 0),
                        COUNT_IF(b.revenue > 0 AND b.revenue < 1000000),
                        COUNT_IF(b.revenue >= 1000000 AND b.revenue < 10000000),
                        COUNT_IF(b.revenue >= 10000000 AND b.revenue < 50000000),
                        COUNT_IF(b.revenue >= 50000000 AND b.revenue < 100000000),
                        COUNT_IF(b.revenue >= 100000000 AND b.revenue < 500000000),
                        COUNT_IF(b.revenue >= 500000000)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 3. RUNTIME (หน่วย: นาที)
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.runtime, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.runtime, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.runtime, 0.75) AS JSON),
                CAST(MAX(b.runtime) AS JSON),
                CAST(MAX_BY(b.title, b.runtime) AS JSON),
                CAST(MAP(
                    ARRAY['0', '<60m', '60-90m', '90-120m', '120-150m', '150-180m', '>180m'],
                    ARRAY[
                        COUNT_IF(b.runtime = 0),
                        COUNT_IF(b.runtime > 0 AND b.runtime < 60),
                        COUNT_IF(b.runtime >= 60 AND b.runtime < 90),
                        COUNT_IF(b.runtime >= 90 AND b.runtime < 120),
                        COUNT_IF(b.runtime >= 120 AND b.runtime < 150),
                        COUNT_IF(b.runtime >= 150 AND b.runtime < 180),
                        COUNT_IF(b.runtime >= 180)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 4. PROFIT
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.profit, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.profit, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.profit, 0.75) AS JSON),
                CAST(MAX(b.profit) AS JSON),
                CAST(MAX_BY(b.title, b.profit) AS JSON),
                CAST(MAP(
                    ARRAY['<0 (ขาดทุน)', '0', '0-10M', '10M-50M', '50M-100M', '100M-500M', '>500M'],
                    ARRAY[
                        COUNT_IF(b.profit < 0),
                        COUNT_IF(b.profit = 0),
                        COUNT_IF(b.profit > 0 AND b.profit < 10000000),
                        COUNT_IF(b.profit >= 10000000 AND b.profit < 50000000),
                        COUNT_IF(b.profit >= 50000000 AND b.profit < 100000000),
                        COUNT_IF(b.profit >= 100000000 AND b.profit < 500000000),
                        COUNT_IF(b.profit >= 500000000)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 5. ROI
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.roi, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.roi, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.roi, 0.75) AS JSON),
                CAST(MAX(b.roi) AS JSON),
                CAST(MAX_BY(b.title, b.roi) AS JSON),
                CAST(MAP(
                    ARRAY['<0%', '0%', '0-50%', '50-100%', '100-300%', '300-1000%', '>1000%'],
                    ARRAY[
                        COUNT_IF(b.roi < 0),
                        COUNT_IF(b.roi = 0),
                        COUNT_IF(b.roi > 0 AND b.roi < 0.5),
                        COUNT_IF(b.roi >= 0.5 AND b.roi < 1.0),
                        COUNT_IF(b.roi >= 1.0 AND b.roi < 3.0),
                        COUNT_IF(b.roi >= 3.0 AND b.roi < 10.0),
                        COUNT_IF(b.roi >= 10.0)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 6. REVENUE MULTIPLIER
        CAST(MAP(
            ARRAY[
                'p25', 'median', 'p75', 'max_val', 'top_movie',
                'profitable_count', 'unprofitable_count', 'profitable_percent', 'unprofitable_percent',
                'bins'
            ],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.revenue_multiplier, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue_multiplier, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue_multiplier, 0.75) AS JSON),
                CAST(MAX(b.revenue_multiplier) AS JSON),
                CAST(MAX_BY(b.title, b.revenue_multiplier) AS JSON),
                CAST(COUNT_IF(b.revenue_multiplier >= 2.5) AS JSON),
                CAST(COUNT_IF(b.revenue_multiplier < 2.5) AS JSON),
                CAST(ROUND(COUNT_IF(b.revenue_multiplier >= 2.5) * 100.0 / NULLIF(COUNT(b.revenue_multiplier), 0), 2) AS JSON),
                CAST(ROUND(COUNT_IF(b.revenue_multiplier < 2.5) * 100.0 / NULLIF(COUNT(b.revenue_multiplier), 0), 2) AS JSON),
                CAST(MAP(
                    ARRAY['0x', '<1x', '1x-2x', '2x-5x', '5x-10x', '>10x'],
                    ARRAY[
                        COUNT_IF(b.revenue_multiplier = 0),
                        COUNT_IF(b.revenue_multiplier > 0 AND b.revenue_multiplier < 1),
                        COUNT_IF(b.revenue_multiplier >= 1 AND b.revenue_multiplier < 2),
                        COUNT_IF(b.revenue_multiplier >= 2 AND b.revenue_multiplier < 5),
                        COUNT_IF(b.revenue_multiplier >= 5 AND b.revenue_multiplier < 10),
                        COUNT_IF(b.revenue_multiplier >= 10)
                    ]
                ) AS JSON)
            ]
        ) AS JSON)
    ]
) AS JSON))
 AS distribution_metrics
    FROM base_movies b
    JOIN "awsdatacatalog"."marts"."bridge_movie_country" bc 
        ON b.movie_sur_id = bc.movie_sur_id 
        AND CAST(bc.country_sur_id AS VARCHAR) NOT IN ('-1', '-2')
    JOIN "awsdatacatalog"."marts"."dim_countries" c 
        ON bc.country_sur_id = c.country_sur_id
    GROUP BY GROUPING SETS (
        (c.iso_3166_1, c.country_name),
        (b.release_quarter, c.iso_3166_1, c.country_name),
        (b.release_quarter, b.release_month, c.iso_3166_1, c.country_name),
        (b.release_year, c.iso_3166_1, c.country_name),
        (b.release_year, b.release_quarter, c.iso_3166_1, c.country_name),
        (b.release_year, b.release_quarter, b.release_month, c.iso_3166_1, c.country_name)
    )
),

-- =========================================================================
-- ส่วนที่ B: genre != 'ALL'
-- =========================================================================

genre_time_agg AS (
    SELECT
        g.genre,
        COALESCE(CAST(b.release_year AS VARCHAR), 'ALL') AS release_year,
        COALESCE(CAST(b.release_quarter AS VARCHAR), 'ALL') AS release_quarter,
        COALESCE(CAST(b.release_month AS VARCHAR), 'ALL') AS release_month,
        'ALL' AS iso_3166_1,
        'ALL' AS country_name,
        COUNT(b.movie_sur_id) AS total_movies,
        ROUND(AVG(b.budget), 2) AS avg_budget,
        ROUND(AVG(b.revenue), 2) AS avg_revenue,
        ROUND(AVG(b.profit), 2) AS avg_profit,
        ROUND(AVG(b.roi), 4) AS avg_roi,
        ROUND(AVG(b.revenue_multiplier), 4) AS avg_revenue_multiplier,
        ROUND(AVG(b.runtime), 2) AS avg_runtime,
        
json_format(CAST(MAP(
    ARRAY['budget', 'revenue', 'runtime', 'profit', 'roi', 'revenue_multiplier'],
    ARRAY[
        -- 1. BUDGET
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.budget, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.budget, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.budget, 0.75) AS JSON),
                CAST(MAX(b.budget) AS JSON),
                CAST(MAX_BY(b.title, b.budget) AS JSON),
                CAST(MAP(
                    ARRAY['0', '<1M', '1M-10M', '10M-50M', '50M-100M', '100M-200M', '>200M'],
                    ARRAY[
                        COUNT_IF(b.budget = 0),
                        COUNT_IF(b.budget > 0 AND b.budget < 1000000),
                        COUNT_IF(b.budget >= 1000000 AND b.budget < 10000000),
                        COUNT_IF(b.budget >= 10000000 AND b.budget < 50000000),
                        COUNT_IF(b.budget >= 50000000 AND b.budget < 100000000),
                        COUNT_IF(b.budget >= 100000000 AND b.budget < 200000000),
                        COUNT_IF(b.budget >= 200000000)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 2. REVENUE
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.revenue, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue, 0.75) AS JSON),
                CAST(MAX(b.revenue) AS JSON),
                CAST(MAX_BY(b.title, b.revenue) AS JSON),
                CAST(MAP(
                    ARRAY['0', '<1M', '1M-10M', '10M-50M', '50M-100M', '100M-500M', '>500M'],
                    ARRAY[
                        COUNT_IF(b.revenue = 0),
                        COUNT_IF(b.revenue > 0 AND b.revenue < 1000000),
                        COUNT_IF(b.revenue >= 1000000 AND b.revenue < 10000000),
                        COUNT_IF(b.revenue >= 10000000 AND b.revenue < 50000000),
                        COUNT_IF(b.revenue >= 50000000 AND b.revenue < 100000000),
                        COUNT_IF(b.revenue >= 100000000 AND b.revenue < 500000000),
                        COUNT_IF(b.revenue >= 500000000)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 3. RUNTIME (หน่วย: นาที)
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.runtime, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.runtime, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.runtime, 0.75) AS JSON),
                CAST(MAX(b.runtime) AS JSON),
                CAST(MAX_BY(b.title, b.runtime) AS JSON),
                CAST(MAP(
                    ARRAY['0', '<60m', '60-90m', '90-120m', '120-150m', '150-180m', '>180m'],
                    ARRAY[
                        COUNT_IF(b.runtime = 0),
                        COUNT_IF(b.runtime > 0 AND b.runtime < 60),
                        COUNT_IF(b.runtime >= 60 AND b.runtime < 90),
                        COUNT_IF(b.runtime >= 90 AND b.runtime < 120),
                        COUNT_IF(b.runtime >= 120 AND b.runtime < 150),
                        COUNT_IF(b.runtime >= 150 AND b.runtime < 180),
                        COUNT_IF(b.runtime >= 180)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 4. PROFIT
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.profit, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.profit, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.profit, 0.75) AS JSON),
                CAST(MAX(b.profit) AS JSON),
                CAST(MAX_BY(b.title, b.profit) AS JSON),
                CAST(MAP(
                    ARRAY['<0 (ขาดทุน)', '0', '0-10M', '10M-50M', '50M-100M', '100M-500M', '>500M'],
                    ARRAY[
                        COUNT_IF(b.profit < 0),
                        COUNT_IF(b.profit = 0),
                        COUNT_IF(b.profit > 0 AND b.profit < 10000000),
                        COUNT_IF(b.profit >= 10000000 AND b.profit < 50000000),
                        COUNT_IF(b.profit >= 50000000 AND b.profit < 100000000),
                        COUNT_IF(b.profit >= 100000000 AND b.profit < 500000000),
                        COUNT_IF(b.profit >= 500000000)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 5. ROI
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.roi, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.roi, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.roi, 0.75) AS JSON),
                CAST(MAX(b.roi) AS JSON),
                CAST(MAX_BY(b.title, b.roi) AS JSON),
                CAST(MAP(
                    ARRAY['<0%', '0%', '0-50%', '50-100%', '100-300%', '300-1000%', '>1000%'],
                    ARRAY[
                        COUNT_IF(b.roi < 0),
                        COUNT_IF(b.roi = 0),
                        COUNT_IF(b.roi > 0 AND b.roi < 0.5),
                        COUNT_IF(b.roi >= 0.5 AND b.roi < 1.0),
                        COUNT_IF(b.roi >= 1.0 AND b.roi < 3.0),
                        COUNT_IF(b.roi >= 3.0 AND b.roi < 10.0),
                        COUNT_IF(b.roi >= 10.0)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 6. REVENUE MULTIPLIER
        CAST(MAP(
            ARRAY[
                'p25', 'median', 'p75', 'max_val', 'top_movie',
                'profitable_count', 'unprofitable_count', 'profitable_percent', 'unprofitable_percent',
                'bins'
            ],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.revenue_multiplier, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue_multiplier, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue_multiplier, 0.75) AS JSON),
                CAST(MAX(b.revenue_multiplier) AS JSON),
                CAST(MAX_BY(b.title, b.revenue_multiplier) AS JSON),
                CAST(COUNT_IF(b.revenue_multiplier >= 2.5) AS JSON),
                CAST(COUNT_IF(b.revenue_multiplier < 2.5) AS JSON),
                CAST(ROUND(COUNT_IF(b.revenue_multiplier >= 2.5) * 100.0 / NULLIF(COUNT(b.revenue_multiplier), 0), 2) AS JSON),
                CAST(ROUND(COUNT_IF(b.revenue_multiplier < 2.5) * 100.0 / NULLIF(COUNT(b.revenue_multiplier), 0), 2) AS JSON),
                CAST(MAP(
                    ARRAY['0x', '<1x', '1x-2x', '2x-5x', '5x-10x', '>10x'],
                    ARRAY[
                        COUNT_IF(b.revenue_multiplier = 0),
                        COUNT_IF(b.revenue_multiplier > 0 AND b.revenue_multiplier < 1),
                        COUNT_IF(b.revenue_multiplier >= 1 AND b.revenue_multiplier < 2),
                        COUNT_IF(b.revenue_multiplier >= 2 AND b.revenue_multiplier < 5),
                        COUNT_IF(b.revenue_multiplier >= 5 AND b.revenue_multiplier < 10),
                        COUNT_IF(b.revenue_multiplier >= 10)
                    ]
                ) AS JSON)
            ]
        ) AS JSON)
    ]
) AS JSON))
 AS distribution_metrics
    FROM base_movies b
    JOIN "awsdatacatalog"."marts"."bridge_movie_genre" bg 
        ON b.movie_sur_id = bg.movie_sur_id 
        AND CAST(bg.genre_sur_id AS VARCHAR) NOT IN ('-1', '-2')
    JOIN "awsdatacatalog"."marts"."dim_genres" g 
        ON bg.genre_sur_id = g.genre_sur_id
    GROUP BY GROUPING SETS (
        (g.genre),
        (g.genre, b.release_quarter),
        (g.genre, b.release_quarter, b.release_month),
        (g.genre, b.release_year),
        (g.genre, b.release_year, b.release_quarter),
        (g.genre, b.release_year, b.release_quarter, b.release_month)
    )
),

genre_country_agg AS (
    SELECT
        g.genre,
        COALESCE(CAST(b.release_year AS VARCHAR), 'ALL') AS release_year,
        COALESCE(CAST(b.release_quarter AS VARCHAR), 'ALL') AS release_quarter,
        COALESCE(CAST(b.release_month AS VARCHAR), 'ALL') AS release_month,
        c.iso_3166_1,
        c.country_name,
        COUNT(b.movie_sur_id) AS total_movies,
        ROUND(AVG(b.budget), 2) AS avg_budget,
        ROUND(AVG(b.revenue), 2) AS avg_revenue,
        ROUND(AVG(b.profit), 2) AS avg_profit,
        ROUND(AVG(b.roi), 4) AS avg_roi,
        ROUND(AVG(b.revenue_multiplier), 4) AS avg_revenue_multiplier,
        ROUND(AVG(b.runtime), 2) AS avg_runtime,
        
json_format(CAST(MAP(
    ARRAY['budget', 'revenue', 'runtime', 'profit', 'roi', 'revenue_multiplier'],
    ARRAY[
        -- 1. BUDGET
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.budget, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.budget, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.budget, 0.75) AS JSON),
                CAST(MAX(b.budget) AS JSON),
                CAST(MAX_BY(b.title, b.budget) AS JSON),
                CAST(MAP(
                    ARRAY['0', '<1M', '1M-10M', '10M-50M', '50M-100M', '100M-200M', '>200M'],
                    ARRAY[
                        COUNT_IF(b.budget = 0),
                        COUNT_IF(b.budget > 0 AND b.budget < 1000000),
                        COUNT_IF(b.budget >= 1000000 AND b.budget < 10000000),
                        COUNT_IF(b.budget >= 10000000 AND b.budget < 50000000),
                        COUNT_IF(b.budget >= 50000000 AND b.budget < 100000000),
                        COUNT_IF(b.budget >= 100000000 AND b.budget < 200000000),
                        COUNT_IF(b.budget >= 200000000)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 2. REVENUE
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.revenue, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue, 0.75) AS JSON),
                CAST(MAX(b.revenue) AS JSON),
                CAST(MAX_BY(b.title, b.revenue) AS JSON),
                CAST(MAP(
                    ARRAY['0', '<1M', '1M-10M', '10M-50M', '50M-100M', '100M-500M', '>500M'],
                    ARRAY[
                        COUNT_IF(b.revenue = 0),
                        COUNT_IF(b.revenue > 0 AND b.revenue < 1000000),
                        COUNT_IF(b.revenue >= 1000000 AND b.revenue < 10000000),
                        COUNT_IF(b.revenue >= 10000000 AND b.revenue < 50000000),
                        COUNT_IF(b.revenue >= 50000000 AND b.revenue < 100000000),
                        COUNT_IF(b.revenue >= 100000000 AND b.revenue < 500000000),
                        COUNT_IF(b.revenue >= 500000000)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 3. RUNTIME (หน่วย: นาที)
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.runtime, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.runtime, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.runtime, 0.75) AS JSON),
                CAST(MAX(b.runtime) AS JSON),
                CAST(MAX_BY(b.title, b.runtime) AS JSON),
                CAST(MAP(
                    ARRAY['0', '<60m', '60-90m', '90-120m', '120-150m', '150-180m', '>180m'],
                    ARRAY[
                        COUNT_IF(b.runtime = 0),
                        COUNT_IF(b.runtime > 0 AND b.runtime < 60),
                        COUNT_IF(b.runtime >= 60 AND b.runtime < 90),
                        COUNT_IF(b.runtime >= 90 AND b.runtime < 120),
                        COUNT_IF(b.runtime >= 120 AND b.runtime < 150),
                        COUNT_IF(b.runtime >= 150 AND b.runtime < 180),
                        COUNT_IF(b.runtime >= 180)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 4. PROFIT
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.profit, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.profit, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.profit, 0.75) AS JSON),
                CAST(MAX(b.profit) AS JSON),
                CAST(MAX_BY(b.title, b.profit) AS JSON),
                CAST(MAP(
                    ARRAY['<0 (ขาดทุน)', '0', '0-10M', '10M-50M', '50M-100M', '100M-500M', '>500M'],
                    ARRAY[
                        COUNT_IF(b.profit < 0),
                        COUNT_IF(b.profit = 0),
                        COUNT_IF(b.profit > 0 AND b.profit < 10000000),
                        COUNT_IF(b.profit >= 10000000 AND b.profit < 50000000),
                        COUNT_IF(b.profit >= 50000000 AND b.profit < 100000000),
                        COUNT_IF(b.profit >= 100000000 AND b.profit < 500000000),
                        COUNT_IF(b.profit >= 500000000)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 5. ROI
        CAST(MAP(
            ARRAY['p25', 'median', 'p75', 'max_val', 'top_movie', 'bins'],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.roi, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.roi, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.roi, 0.75) AS JSON),
                CAST(MAX(b.roi) AS JSON),
                CAST(MAX_BY(b.title, b.roi) AS JSON),
                CAST(MAP(
                    ARRAY['<0%', '0%', '0-50%', '50-100%', '100-300%', '300-1000%', '>1000%'],
                    ARRAY[
                        COUNT_IF(b.roi < 0),
                        COUNT_IF(b.roi = 0),
                        COUNT_IF(b.roi > 0 AND b.roi < 0.5),
                        COUNT_IF(b.roi >= 0.5 AND b.roi < 1.0),
                        COUNT_IF(b.roi >= 1.0 AND b.roi < 3.0),
                        COUNT_IF(b.roi >= 3.0 AND b.roi < 10.0),
                        COUNT_IF(b.roi >= 10.0)
                    ]
                ) AS JSON)
            ]
        ) AS JSON),

        -- 6. REVENUE MULTIPLIER
        CAST(MAP(
            ARRAY[
                'p25', 'median', 'p75', 'max_val', 'top_movie',
                'profitable_count', 'unprofitable_count', 'profitable_percent', 'unprofitable_percent',
                'bins'
            ],
            ARRAY[
                CAST(APPROX_PERCENTILE(b.revenue_multiplier, 0.25) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue_multiplier, 0.50) AS JSON),
                CAST(APPROX_PERCENTILE(b.revenue_multiplier, 0.75) AS JSON),
                CAST(MAX(b.revenue_multiplier) AS JSON),
                CAST(MAX_BY(b.title, b.revenue_multiplier) AS JSON),
                CAST(COUNT_IF(b.revenue_multiplier >= 2.5) AS JSON),
                CAST(COUNT_IF(b.revenue_multiplier < 2.5) AS JSON),
                CAST(ROUND(COUNT_IF(b.revenue_multiplier >= 2.5) * 100.0 / NULLIF(COUNT(b.revenue_multiplier), 0), 2) AS JSON),
                CAST(ROUND(COUNT_IF(b.revenue_multiplier < 2.5) * 100.0 / NULLIF(COUNT(b.revenue_multiplier), 0), 2) AS JSON),
                CAST(MAP(
                    ARRAY['0x', '<1x', '1x-2x', '2x-5x', '5x-10x', '>10x'],
                    ARRAY[
                        COUNT_IF(b.revenue_multiplier = 0),
                        COUNT_IF(b.revenue_multiplier > 0 AND b.revenue_multiplier < 1),
                        COUNT_IF(b.revenue_multiplier >= 1 AND b.revenue_multiplier < 2),
                        COUNT_IF(b.revenue_multiplier >= 2 AND b.revenue_multiplier < 5),
                        COUNT_IF(b.revenue_multiplier >= 5 AND b.revenue_multiplier < 10),
                        COUNT_IF(b.revenue_multiplier >= 10)
                    ]
                ) AS JSON)
            ]
        ) AS JSON)
    ]
) AS JSON))
 AS distribution_metrics
    FROM base_movies b
    JOIN "awsdatacatalog"."marts"."bridge_movie_genre" bg 
        ON b.movie_sur_id = bg.movie_sur_id 
        AND CAST(bg.genre_sur_id AS VARCHAR) NOT IN ('-1', '-2')
    JOIN "awsdatacatalog"."marts"."dim_genres" g 
        ON bg.genre_sur_id = g.genre_sur_id
    JOIN "awsdatacatalog"."marts"."bridge_movie_country" bc 
        ON b.movie_sur_id = bc.movie_sur_id 
        AND CAST(bc.country_sur_id AS VARCHAR) NOT IN ('-1', '-2')
    JOIN "awsdatacatalog"."marts"."dim_countries" c 
        ON bc.country_sur_id = c.country_sur_id
    GROUP BY GROUPING SETS (
        (g.genre, c.iso_3166_1, c.country_name),
        (g.genre, b.release_quarter, c.iso_3166_1, c.country_name),
        (g.genre, b.release_quarter, b.release_month, c.iso_3166_1, c.country_name),
        (g.genre, b.release_year, c.iso_3166_1, c.country_name),
        (g.genre, b.release_year, b.release_quarter, c.iso_3166_1, c.country_name),
        (g.genre, b.release_year, b.release_quarter, b.release_month, c.iso_3166_1, c.country_name)
    )
)

SELECT
    genre,
    release_quarter,
    release_month,
    iso_3166_1,
    country_name,
    total_movies,
    avg_budget,
    avg_revenue,
    avg_profit,
    avg_roi,
    avg_revenue_multiplier,
    avg_runtime,
    distribution_metrics,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS calculated_at,
    release_year
FROM (
    SELECT * FROM all_genre_time_agg
    UNION ALL SELECT * FROM all_genre_country_agg
    UNION ALL SELECT * FROM genre_time_agg
    UNION ALL SELECT * FROM genre_country_agg
)
ORDER BY genre, release_quarter, release_month, country_name