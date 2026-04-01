-- part of a query repo
-- query name: Balancer Daily Liquidity Utilization
-- query link: https://dune.com/queries/2648081


WITH labels AS (
        SELECT * FROM (SELECT
            address,
            name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.addresses
        WHERE "category" IN ('balancer_pool', 'balancer_v2_pool', 'balancer_v3_pool')
        GROUP BY 1, 2) l
        WHERE num = 1
    ), 
    
    swaps AS (
        SELECT
            date_trunc('day', d.block_time) AS day,
            SUM(amount_usd) AS volume
        FROM dex.trades d
        WHERE project = 'balancer' AND version = '2'
        AND ('{{1. Pool ID}}' = 'All' OR CAST(project_contract_address as varchar) = SUBSTRING('{{1. Pool ID}}',1,42))
        GROUP BY 1
    ),

    prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, AVG(price) AS price
        FROM prices.usd
        WHERE minute > TIMESTAMP '2021-04-20'
        GROUP BY 1, 2
    ),
    
    dex_prices_1 AS (
        SELECT date_trunc('day', hour) AS day, 
        contract_address AS token, 
        approx_percentile(median_price, 0.5) AS price,
        SUM(sample_size) as sample_size
        FROM dex.prices
        WHERE hour > TIMESTAMP '2021-04-20'
        GROUP BY 1, 2
        HAVING sum(sample_size) > 3
    ),
    
    dex_prices AS (
        SELECT *, LEAD(day, 1, now()) OVER (PARTITION BY token ORDER BY day) AS day_of_next_change
        FROM dex_prices_1
    ),
    
    swaps_changes AS (
        SELECT day, pool, token, SUM(COALESCE(delta, 0)) AS delta FROM (
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, "tokenIn" AS token, CAST("amountIn" as double) AS delta
        FROM balancer_v2_{{4. Blockchain}}.Vault_evt_Swap
        UNION ALL
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, "tokenOut" AS token, -CAST("amountOut" as double) AS delta
        FROM balancer_v2_{{4. Blockchain}}.Vault_evt_Swap) swaps
        GROUP BY 1, 2, 3
    ),
    
    internal_changes AS (
        SELECT date_trunc('day', evt_block_time) AS day, '0xBA12222222228d8Ba445958a75a0704d566BF2C8' AS pool, token, SUM(COALESCE(CAST(delta as double), CAST(0 as double))) AS delta 
        FROM balancer_v2_{{4. Blockchain}}.Vault_evt_InternalBalanceChanged
        GROUP BY 1, 2, 3
    ),
    
    balances_changes AS (
    SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, t.token, t.delta
    FROM balancer_v2_{{4. Blockchain}}.Vault_evt_PoolBalanceChanged
    CROSS JOIN UNNEST("tokens") as t(token)
    CROSS JOIN UNNEST("deltas") as t(delta)
    ),
    
    managed_changes AS (
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, token, "managedDelta" AS delta
        FROM balancer_v2_{{4. Blockchain}}.Vault_evt_PoolBalanceManaged
    ),
    
    daily_delta_balance AS (
         SELECT day, pool, token, SUM(COALESCE(amount, 0)) AS amount 
        FROM (
            SELECT day, CAST(pool as varchar) as pool, token, SUM(COALESCE(CAST(delta as double), CAST(0 as double))) AS amount 
            FROM balances_changes
            GROUP BY 1, 2, 3
            UNION ALL
            SELECT day, CAST(pool as varchar) as pool, token, CAST(delta as double) AS amount 
            FROM swaps_changes
            UNION ALL
            SELECT day, CAST(pool as varchar) as pool, token, CAST(delta as double) AS amount 
            FROM internal_changes
            UNION ALL
            SELECT day, CAST(pool as varchar) as pool, token, CAST(delta as double) AS amount 
            FROM managed_changes
            ) balance
        WHERE ('{{1. Pool ID}}' = 'All' OR
       SUBSTRING(CAST(pool as varchar), 1, 42) = '{{1. Pool ID}}')
        GROUP BY 1, 2, 3
    ),
    
    cumulative_balance AS (
        SELECT 
            day,
            pool, 
            token,
            LEAD(day, 1, now()) OVER (PARTITION BY token, pool ORDER BY day) AS day_of_next_change,
            SUM(amount) OVER (PARTITION BY pool, token ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
        FROM daily_delta_balance
    ),
    
    dayly_delta_balance_by_token AS (
        SELECT day, pool, token, cumulative_amount, (cumulative_amount - COALESCE(LAG(cumulative_amount, 1) OVER (PARTITION BY pool, token ORDER BY day), 0)) AS amount
        FROM (SELECT day, pool, token, SUM(cumulative_amount) AS cumulative_amount
        FROM cumulative_balance b
        WHERE extract(dow from day) = 1
        GROUP BY 1, 2, 3) foo
    ),
    
    calendar AS (
      
    with days_seq as (
        SELECT
        sequence(
            (SELECT cast(min(date_trunc('day', evt_block_time)) as timestamp) day FROM erc20_ethereum.evt_Transfer tr)
            , date_trunc('day', cast(now() as timestamp))
            , interval '1' day) as day
    )
    
    SELECT 
        days.day
    FROM days_seq
    CROSS JOIN unnest(day) as days(day)
    ),
    
    cumulative_usd_balance AS (
        SELECT c.day, b.pool, b.token, cumulative_amount,
        cumulative_amount / power(10, t.decimals) * p1.price AS amount_usd_from_api,
        cumulative_amount /power(10, t.decimals) * p2.price AS amount_usd_from_dex
        FROM calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day AND c.day < b.day_of_next_change
        LEFT JOIN tokens.erc20 t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= c.day AND c.day < p2.day_of_next_change AND p2.token = b.token
    ),
    
    estimated_pool_liquidity as (
        SELECT 
            day, 
            pool, 
            SUM(COALESCE(amount_usd_from_api, amount_usd_from_dex)) AS liquidity
        FROM cumulative_usd_balance
        GROUP BY 1, 2
    ),

    total_tvl AS (
        SELECT date_trunc('day', day) AS day, SUM(protocol_liquidity_usd) AS tvl
        FROM balancer_v2_ethereum.liquidity
         WHERE ('{{1. Pool ID}}' = 'All' OR
       SUBSTRING(CAST(pool_id as varchar), 1, 42) = '{{1. Pool ID}}')
        GROUP BY 1
    )
   
SELECT
    CAST(t.day as timestamp) as day,
    (s.volume)/(t.tvl) AS ratio,
    s.volume,
    t.tvl
FROM total_tvl t
JOIN swaps s ON s.day = t.day
WHERE t.day >= TIMESTAMP '{{2. Start date}}'
AND t.day <= TIMESTAMP '{{3. End date}}'
ORDER BY 1