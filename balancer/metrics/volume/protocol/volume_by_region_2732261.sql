-- part of a query repo
-- query name: Volume by Region
-- query link: https://dune.com/queries/2732261


WITH 
    zones AS (
        SELECT *,
            CASE WHEN hour < 8 AND hour >= 0 THEN 1 -- '0-8 UTC'
            WHEN hour < 16 AND hour >= 8 THEN 2 -- '8-16 UTC'
            WHEN hour <= 23 AND hour >= 16 THEN 3 -- '16-24 UTC'
            END AS zone
        FROM ( 
            SELECT distinct block_date, hour(block_time) AS hour, amount_usd
            FROM balancer.trades 
            )
        WHERE block_date >= current_date - interval '{{Date Range in Days}}' day
        ),
    base AS (
        SELECT 
            block_date, 
            zone, 
            sum(amount_usd) AS volume, 
            sum(sum(amount_usd)) OVER(PARTITION BY zone ORDER BY block_date ROWS UNBOUNDED PRECEDING) AS cum_vol
        FROM zones
        GROUP BY 1, 2
        ORDER BY 1 DESC, 3 DESC
        ),
    asia AS (
        SELECT block_date, volume AS asia_vol, cum_vol AS asia_cum_vol
        FROM base
        WHERE zone = 1
        ),
    europe AS (
        SELECT block_date, volume AS europe_vol, cum_vol AS europe_cum_vol
        FROM base
        WHERE zone = 2
        ),
    americas AS (
        SELECT block_date, volume AS americas_vol, cum_vol AS americas_cum_vol
        FROM base
        WHERE zone = 3
        ),
    cal AS (
        SELECT
            date_add('day', step, day) AS day
        FROM
            UNNEST(
                SEQUENCE(
                    date_trunc('day', cast((current_date - interval '{{Date Range in Days}}' day) AS TIMESTAMP) - INTERVAL '1' day),
                    date_trunc('day', current_date) + INTERVAL '1' day,
                    INTERVAL '1' day
                )
            ) AS t(day)
        CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 1, 1)) AS t(step) )
        WHERE date_add('day', step, day) <= date_trunc('day', current_date )
    )
        
SELECT * FROM cal c
LEFT JOIN asia a ON a.block_date = c.day
LEFT JOIN americas aa ON aa.block_date = c.day
LEFT JOIN europe e ON e.block_date = c.day
ORDER BY c.day DESC




