-- part of a query repo
-- query name: bcowamm balances by tx
-- query link: https://dune.com/queries/3957792


WITH pools AS (
    SELECT 
        bPool AS pools
    FROM b_cow_amm_ethereum.BCoWFactory_evt_LOG_NEW_POOL
),

joins AS (
    SELECT 
        p.pools AS pool, 
        e.evt_block_time, 
        e.contract_address AS token, 
        SUM(CAST(value AS int256)) AS amount
    FROM erc20_ethereum.evt_transfer e
    INNER JOIN pools p ON e."to" = p.pools
    GROUP BY 1, 2, 3
),

exits AS (
    SELECT 
        p.pools AS pool, 
        e.evt_block_time, 
        e.contract_address AS token, 
        - SUM(CAST(value AS int256)) AS amount
    FROM erc20_ethereum.evt_transfer e
    INNER JOIN pools p ON e."from" = p.pools   
    GROUP BY 1, 2, 3
),

daily_delta_balance_by_token AS (
    SELECT 
        pool, 
        evt_block_time, 
        token, 
        SUM(COALESCE(amount, CAST(0 AS int256))) AS amount 
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
        evt_block_time, 
        LEAD(evt_block_time, 1, now()) OVER (PARTITION BY pool, token ORDER BY evt_block_time) AS evt_block_time_of_next_change,
        SUM(amount) OVER (PARTITION BY pool, token ORDER BY evt_block_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM daily_delta_balance_by_token)


SELECT
    evt_block_time, 
    b.pool AS pool_address, 
    b.token AS token_address, 
    (b.cumulative_amount * price) / POWER(10, p.decimals) AS token_balance_usd
FROM cumulative_balance_by_token b 
LEFT JOIN prices.usd p ON b.token = p.contract_address
AND p.blockchain = 'ethereum'
AND DATE_TRUNC('minute', b.evt_block_time) = p.minute