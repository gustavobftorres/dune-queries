-- part of a query repo
-- query name: bcow_pool_balances
-- query link: https://dune.com/queries/3870935


WITH pools AS (
    SELECT 
        pool as pools
    FROM balancer_testnet_sepolia.BCOWFactory_evt_LOG_NEW_POOL
),

joins AS (
    SELECT 
        p.pools as pool, 
        date_trunc('day', e.evt_block_time) AS day, 
        e.contract_address AS token, 
        SUM(CAST(value AS int256)) AS amount
    FROM erc20_sepolia.evt_transfer e
    INNER JOIN pools p ON e."to" = p.pools
    GROUP BY 1, 2, 3
),

exits AS (
    SELECT 
        p.pools as pool, 
        date_trunc('day', e.evt_block_time) AS day, 
        e.contract_address AS token, 
        - SUM(CAST(value as int256)) AS amount
    FROM erc20_sepolia.evt_transfer e
    INNER JOIN pools p ON e."from" = p.pools   
    GROUP BY 1, 2, 3
),

daily_delta_balance_by_token AS (
    SELECT 
        pool, 
        day, 
        token, 
        SUM(COALESCE(amount, CAST(0 as int256))) AS amount 
    FROM 
        (SELECT *
        FROM joins j
        UNION ALL
        SELECT * 
        FROM exits e) foo
    GROUP BY 1, 2, 3
),

cumulative_balance_by_token AS (
    SELECT
        pool, 
        token, 
        day, 
        LEAD(day, 1, now()) OVER (PARTITION BY pool, token ORDER BY day) AS day_of_next_change,
        SUM(amount) OVER (PARTITION BY pool, token ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM daily_delta_balance_by_token
),

    calendar AS (
        SELECT 
            date_sequence AS day
        FROM unnest(sequence(date('2024-05-01'), date(now()), interval '1' day)) as t(date_sequence)
    ),


running_cumulative_balance_by_token AS (
    SELECT 
        c.day, 
        pool, 
        token, 
        cumulative_amount, 
        cumulative_amount AS cumulative_amount_raw,  
        cumulative_amount / POWER(10,18) AS cumulative_amount 
    FROM calendar c
    LEFT JOIN cumulative_balance_by_token b ON b.day <= c.day AND c.day < b.day_of_next_change
)

SELECT 
    * 
FROM running_cumulative_balance_by_token
WHERE pool IS NOT NULL
ORDER BY 1 DESC