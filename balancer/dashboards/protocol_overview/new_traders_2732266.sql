-- part of a query repo
-- query name: New Traders
-- query link: https://dune.com/queries/2732266


WITH 
unique_trader_date AS (
    SELECT 
        trader,
        CAST(min(block_date) AS TIMESTAMP) AS min_block_date
    FROM (
            SELECT
                block_date,
                tx_to AS trader
            FROM
                balancer.trades
            UNION
            SELECT
                block_date,
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
WHERE min_block_date >= current_date - interval '{{Date Range in Days}}' day
ORDER BY min_block_date DESC