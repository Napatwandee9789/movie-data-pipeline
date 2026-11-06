WITH source_data AS 
(
    SELECT *,
        -- 1. Profit: ถ้า revenue หรือ budget เป็น NULL จะได้ NULL อัตโนมัติ
        revenue - budget AS profit,

        -- 2. Revenue Multiplier: ถ้า revenue/budget เป็น NULL หรือ budget = 0 ให้ คืนค่าเป็น NULL
        CASE 
            WHEN revenue IS NOT NULL AND budget > 0 
            THEN ROUND(CAST(revenue AS DOUBLE) / budget, 2)
            ELSE NULL 
        END AS revenue_multiplier,

        -- 3. ROI (%): ถ้า revenue/budget เป็น NULL หรือ budget = 0 ให้ คืนค่าเป็น NULL
        CASE 
            WHEN revenue IS NOT NULL AND budget > 0 
            THEN ROUND(((CAST(revenue AS DOUBLE) - budget) / budget) * 100, 2)
            ELSE NULL 
        END AS roi
    FROM {{ ref('stg_movie_historical') }}
)

SELECT * FROM source_data