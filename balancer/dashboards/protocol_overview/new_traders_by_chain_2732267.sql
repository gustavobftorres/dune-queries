-- part of a query repo
-- query name: New Traders by Chain
-- query link: https://dune.com/queries/2732267


WITH 
unique_trader_date AS (
    SELECT 
    distinct
        trader,
        blockchain,
        min(block_date) OVER(PARTITION BY trader, blockchain) AS min_block_date
    FROM (
            SELECT
                block_date,
                blockchain,
                tx_to AS trader
            FROM
                balancer.trades
            UNION
            SELECT
                block_date,
                blockchain,
                tx_from AS trader
            FROM
                balancer.trades
    )
), 

unique_trader_stats AS (
    SELECT
        min_block_date,
        blockchain,
        count(trader) AS new_traders,
        sum(count(trader)) OVER(PARTITION BY blockchain ORDER BY min_block_date ROWS UNBOUNDED PRECEDING) AS cum_new_traders 
    FROM unique_trader_date 
    GROUP BY 1, 2
),
base_data AS (
    SELECT *, new_traders - lag(new_traders) OVER(PARTITION BY blockchain ORDER BY min_block_date ASC) AS new_user_trend_change
    FROM unique_trader_stats
),
eth AS (
    SELECT 
        min_block_date, 
        new_traders AS eth_new_traders, 
        cum_new_traders AS eth_cum_new_traders, 
        new_user_trend_change AS eth_new_user_trend_change 
    FROM base_data 
    WHERE blockchain = 'ethereum'
),
arb AS (
    SELECT 
        min_block_date, 
        new_traders AS arb_new_traders, 
        cum_new_traders AS arb_cum_new_traders, 
        new_user_trend_change AS arb_new_user_trend_change 
    FROM base_data 
    WHERE blockchain = 'arbitrum'
),
pol AS (
    SELECT 
        min_block_date, 
        new_traders AS pol_new_traders, 
        cum_new_traders AS pol_cum_new_traders, 
        new_user_trend_change AS pol_new_user_trend_change 
    FROM base_data 
    WHERE blockchain = 'polygon'
),
opt AS (
    SELECT 
        min_block_date, 
        new_traders AS opt_new_traders, 
        cum_new_traders AS opt_cum_new_traders, 
        new_user_trend_change AS opt_new_user_trend_change 
    FROM base_data 
    WHERE blockchain = 'optimism'
),
gno AS (
    SELECT 
        min_block_date, 
        new_traders AS gno_new_traders, 
        cum_new_traders AS gno_cum_new_traders, 
        new_user_trend_change AS gno_new_user_trend_change 
    FROM base_data 
    WHERE blockchain = 'gnosis'
),
bas AS (
    SELECT 
        min_block_date, 
        new_traders AS bas_new_traders, 
        cum_new_traders AS bas_cum_new_traders, 
        new_user_trend_change AS bas_new_user_trend_change 
    FROM base_data 
    WHERE blockchain = 'base'
),
ava AS (
    SELECT 
        min_block_date, 
        new_traders AS ava_new_traders, 
        cum_new_traders AS ava_cum_new_traders, 
        new_user_trend_change AS ava_new_user_trend_change 
    FROM base_data 
    WHERE blockchain = 'avalanche_c'
),
zkevm AS (
    SELECT 
        min_block_date, 
        new_traders AS ava_new_traders, 
        cum_new_traders AS ava_cum_new_traders, 
        new_user_trend_change AS ava_new_user_trend_change 
    FROM base_data 
    WHERE blockchain = 'zkevm'
),
cal AS (
    SELECT
        date_add('day', step, day) AS day
    FROM
        UNNEST(
            SEQUENCE(
                date_trunc('day', cast('2020-03-15' AS TIMESTAMP) - INTERVAL '1' day),
                date_trunc('day', current_date) + INTERVAL '1' day,
                INTERVAL '1' day
            )
        ) AS t(day)
    CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 1, 1)) AS t(step) )
    WHERE date_add('day', step, day) <= date_trunc('day', current_date )
)
SELECT * FROM cal c
LEFT JOIN eth e ON e.min_block_date = c.day
LEFT JOIN arb a ON a.min_block_date = c.day
LEFT JOIN pol p ON p.min_block_date = c.day
LEFT JOIN opt o ON o.min_block_date = c.day
LEFT JOIN gno g ON g.min_block_date = c.day
LEFT JOIN bas b ON b.min_block_date = c.day
LEFT JOIN ava v ON v.min_block_date = c.day
LEFT JOIN zkevm z ON z.min_block_date = c.day
WHERE c.day >= current_date - interval '{{Date Range in Days}}' day