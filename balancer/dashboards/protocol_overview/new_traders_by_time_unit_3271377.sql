-- part of a query repo
-- query name: New Traders by Time Unit
-- query link: https://dune.com/queries/3271377


WITH 
unique_trader_date AS (
    SELECT 
        trader,
        CAST(min(block_date) AS TIMESTAMP) AS min_block_date
    FROM (
            SELECT
                DATE_TRUNC('{{Time Unit}}', block_date) AS block_date,
                tx_to AS trader
            FROM
                balancer.trades
            UNION
            SELECT
                DATE_TRUNC('{{Time Unit}}', block_date) AS block_date,
                tx_from AS trader
            FROM
                balancer.trades
    )
    GROUP BY 1
),
unique_trader_stats AS (
    SELECT
        min_block_date,
        count(trader) AS new_traders,
        sum(count(trader)) OVER(ORDER BY min_block_date ROWS UNBOUNDED PRECEDING) AS cum_new_traders 
    FROM unique_trader_date 
    GROUP BY 1
)
SELECT *, new_traders - lag(new_traders) OVER(ORDER BY min_block_date ASC) AS new_user_trend_change
FROM unique_trader_stats
WHERE
    CASE 
        WHEN '{{Time Unit}}' = 'DAY' THEN min_block_date >= current_date - interval '{{Date Range in Time Units}}' DAY
        WHEN '{{Time Unit}}' = 'WEEK' THEN min_block_date >= date_trunc('{{Time Unit}}', current_date) - (interval '{{Date Range in Time Units}}' DAY) * 7
        WHEN '{{Time Unit}}' = 'MONTH' THEN min_block_date >= date_trunc('{{Time Unit}}', current_date) - interval '{{Date Range in Time Units}}' MONTH
        WHEN '{{Time Unit}}' = 'QUARTER' THEN min_block_date >= date_trunc('{{Time Unit}}', current_date) - (interval '{{Date Range in Time Units}}' MONTH * 3)
    END
ORDER BY min_block_date DESC