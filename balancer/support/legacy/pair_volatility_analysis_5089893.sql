-- part of a query repo
-- query name: Pair Volatility Analysis
-- query link: https://dune.com/queries/5089893


WITH 
price_data AS (
    SELECT 
        date_trunc('hour', minute) AS hour,
        contract_address,
        symbol,
        AVG(price) AS avg_price
    FROM prices.usd
    WHERE 
        blockchain = '{{blockchain}}'
        AND contract_address IN (
            {{token_a}}, 
            {{token_b}}
        )
        AND minute >= CURRENT_DATE - INTERVAL '180' DAY
    GROUP BY 1, 2, 3
),

price_ratio AS (
    SELECT 
        t0.hour,
        t0.avg_price AS token0_price,
        t1.avg_price AS token1_price,
        t0.avg_price / t1.avg_price AS price_ratio
    FROM 
        price_data t0
        JOIN price_data t1 ON t0.hour = t1.hour
    WHERE 
        t0.contract_address = {{token_a}}
        AND t1.contract_address = {{token_b}}
),

time_segments AS (
    SELECT
        hour,
        price_ratio,
        CASE WHEN hour >= CURRENT_DATE - INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_last_7_days,
        CASE WHEN hour >= CURRENT_DATE - INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_last_30_days,
        CASE WHEN hour >= CURRENT_DATE - INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_last_90_days
    FROM price_ratio
),

daily_changes AS (
    SELECT
        hour,
        price_ratio,
        LAG(price_ratio, 24) OVER (ORDER BY hour) AS prev_day_ratio,
        (price_ratio - LAG(price_ratio, 24) OVER (ORDER BY hour)) / 
            NULLIF(LAG(price_ratio, 24) OVER (ORDER BY hour), 0) * 100 AS daily_pct_change
    FROM price_ratio
),

trend_analysis AS (
    SELECT
        AVG(CASE WHEN pr.hour >= CURRENT_DATE - INTERVAL '30' DAY THEN pr.price_ratio ELSE NULL END) - 
        AVG(CASE WHEN pr.hour >= CURRENT_DATE - INTERVAL '60' DAY AND pr.hour < CURRENT_DATE - INTERVAL '30' DAY 
            THEN pr.price_ratio ELSE NULL END) AS trend_30d,
        
        AVG(CASE WHEN pr.hour >= CURRENT_DATE - INTERVAL '7' DAY THEN pr.price_ratio ELSE NULL END) - 
        AVG(CASE WHEN pr.hour >= CURRENT_DATE - INTERVAL '14' DAY AND pr.hour < CURRENT_DATE - INTERVAL '7' DAY 
            THEN pr.price_ratio ELSE NULL END) AS trend_7d
    FROM price_ratio pr
),

price_stats AS (
    SELECT
        (SELECT pr.price_ratio FROM price_ratio pr ORDER BY pr.hour DESC LIMIT 1) AS current_price,
        APPROX_PERCENTILE(CASE WHEN ts.is_last_7_days = 1 THEN ts.price_ratio END, 0.5) AS median_7d,
        APPROX_PERCENTILE(CASE WHEN ts.is_last_30_days = 1 THEN ts.price_ratio END, 0.5) AS median_30d,

        MIN(ts.price_ratio) AS min_price,
        MAX(ts.price_ratio) AS max_price,
        APPROX_PERCENTILE(ts.price_ratio, 0.5) AS median_all_time,
        
        STDDEV(CASE WHEN ts.is_last_7_days = 1 THEN dc.daily_pct_change END) AS volatility_7d,
        STDDEV(CASE WHEN ts.is_last_30_days = 1 THEN dc.daily_pct_change END) AS volatility_30d,
        STDDEV(CASE WHEN ts.is_last_90_days = 1 THEN dc.daily_pct_change END) AS volatility_90d,
        STDDEV(dc.daily_pct_change) AS volatility_all_time,
        
        MAX(CASE WHEN ts.is_last_30_days = 1 THEN ABS(dc.daily_pct_change) END) AS max_daily_move_30d,
        MAX(ABS(dc.daily_pct_change)) AS max_daily_move_all_time,
        
        (SELECT trend_30d FROM trend_analysis) AS trend_30d,
        (SELECT trend_7d FROM trend_analysis) AS trend_7d
    FROM time_segments ts
    LEFT JOIN daily_changes dc ON ts.hour = dc.hour
    WHERE dc.prev_day_ratio IS NOT NULL OR dc.prev_day_ratio IS NULL
    GROUP BY 1
)

SELECT
    current_price,
    median_7d,
    median_30d,
    median_all_time,
    
    volatility_7d,
    volatility_30d,
    volatility_90d,
    volatility_all_time,
    
    trend_7d,
    trend_30d,
    
    min_price,
    max_price,
    
    current_price * (1 - (2 * COALESCE(volatility_7d, volatility_30d, 5)/100 * 5.5)) AS vol_range_lower,
    current_price * (1 + (2 * COALESCE(volatility_7d, volatility_30d, 5)/100 * 5.5)) AS vol_range_upper,
    
    current_price * (1 - (2.5 * COALESCE(volatility_30d, 5)/100 * 5.5)) + 
        (CASE WHEN trend_7d > 0 THEN trend_7d * 2 ELSE 0 END) AS trend_adj_range_lower,
    current_price * (1 + (2.5 * COALESCE(volatility_30d, 5)/100 * 5.5)) + 
        (CASE WHEN trend_7d < 0 THEN trend_7d * 2 ELSE 0 END) AS trend_adj_range_upper,
    
    CASE 
        WHEN COALESCE(volatility_30d, 5) < 2 THEN current_price * 0.9
        WHEN COALESCE(volatility_30d, 5) < 5 THEN current_price * 0.75
        ELSE current_price * 0.5
    END AS adaptive_range_lower,

    CASE 
        WHEN COALESCE(volatility_30d, 5) < 2 THEN current_price * 1.1
        WHEN COALESCE(volatility_30d, 5) < 5 THEN current_price * 1.25
        ELSE current_price * 1.5
    END AS adaptive_range_upper,
    
    CASE 
        WHEN COALESCE(volatility_30d, 5) < 2 THEN 0.10
        WHEN COALESCE(volatility_30d, 5) < 5 THEN 0.25
        ELSE 0.50
    END AS recommended_margin
FROM price_stats
