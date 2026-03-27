-- part of a query repo
-- query name: Balancer LM Pools on Polygon
-- query link: https://dune.com/queries/306087


WITH prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, decimals, AVG(price) AS price
        FROM prices.usd
        GROUP BY 1, 2, 3
    ), 
    
    labels AS (
        SELECT
            address,
            label AS name
        FROM dune_user_generated."balancer_pools"
        WHERE "type" = 'balancer_v2_pool'
        GROUP BY 1, 2
    ),
    
    weekly_rewards AS (
        SELECT date_trunc('week', day) AS week, pool_id, SUM(amount)::int AS bal_amount
        FROM dune_user_generated.balancer_liquidity_mining
        WHERE chain_id = '137'
        GROUP BY 1, 2
    ),
    
    current_incentivized_pools AS (
        SELECT pool_id, date_trunc('day', evt_block_time) AS created_at, bal_amount
        FROM weekly_rewards r
        JOIN balancer_v2."Vault_evt_PoolRegistered" b
        ON b."poolId" = r.pool_id
        WHERE week = date_trunc('week', CURRENT_DATE)
    ),
    
    last_90d_avg_rewards AS (
        SELECT pool_id, AVG(bal_amount)::int AS last_90d_avg_rewards
        FROM weekly_rewards
        WHERE week >= date_trunc('week', CURRENT_DATE - interval '90d')
        GROUP BY 1
    ),
    
    all_time_rewards AS (
        SELECT pool_id, SUM(bal_amount)::int AS all_time_rewards
        FROM weekly_rewards
        GROUP BY 1
    ),
    
    timeframe_rewards AS (
        SELECT pool_id, SUM(amount)::int AS timeframe_rewards
        FROM dune_user_generated.balancer_liquidity_mining
        WHERE chain_id = '137'
        AND day >= '{{1. Start date}}'
        AND day <= '{{2. End date}}'
        GROUP BY 1
    ),
    
    trades AS (
        SELECT *, COALESCE(s1."swapFeePercentage", s2."swapFeePercentage")/1e18 AS swap_fee
        FROM dex.trades s
        LEFT JOIN balancer_v2."WeightedPool_evt_SwapFeePercentageChanged" s1 ON s1.contract_address = SUBSTRING(s.exchange_contract_address, 0, 21)
        AND s1.evt_block_time = (
            SELECT MAX(evt_block_time)
            FROM balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
            WHERE evt_block_time <= s.block_time
            AND contract_address = SUBSTRING(s.exchange_contract_address, 0, 21))
        LEFT JOIN balancer_v2."StablePool_evt_SwapFeePercentageChanged" s2 ON s2.contract_address = SUBSTRING(s.exchange_contract_address, 0, 21)
        AND s2.evt_block_time = (
            SELECT MAX(evt_block_time)
            FROM balancer_v2."StablePool_evt_SwapFeePercentageChanged"
            WHERE evt_block_time <= s.block_time
            AND contract_address = SUBSTRING(s.exchange_contract_address, 0, 21)
        )
        WHERE s.project = 'Balancer'
    ),
    
    last_90d_fees AS (
        SELECT pool_id, SUM(usd_amount * swap_fee) AS last_90d_fees
        FROM trades t
        JOIN current_incentivized_pools p 
        ON p.pool_id = t.exchange_contract_address
        AND t.block_time >= CURRENT_DATE - '90d'::interval
        GROUP BY 1
    ),
    
    last_7d_fees AS (
        SELECT pool_id, bal_amount::int, SUM(usd_amount * swap_fee) AS last_7d_fees
        FROM trades t
        JOIN current_incentivized_pools p 
        ON p.pool_id = t.exchange_contract_address
        AND t.block_time >= CURRENT_DATE - '7d'::interval
        GROUP BY 1, 2
    ),
    
    all_time_fees AS (
        SELECT pool_id, created_at, SUM(usd_amount * swap_fee) AS all_time_fees
        FROM trades t
        JOIN current_incentivized_pools p
        ON p.pool_id = t.exchange_contract_address
        GROUP BY 1, 2
    ),
    
    timeframe_fees AS (
        SELECT pool_id, SUM(usd_amount * swap_fee) AS timeframe_fees
        FROM trades t
        JOIN current_incentivized_pools p 
        ON p.pool_id = t.exchange_contract_address
        AND block_time >= '{{1. Start date}}'
        AND block_time <= '{{2. End date}}'
        GROUP BY 1
    ),
    
    swaps_changes AS (
        SELECT day, pool, token, SUM(COALESCE(delta, 0)) AS delta FROM (
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, "tokenIn" AS token, "amountIn" AS delta
        FROM balancer_v2."Vault_evt_Swap"
        UNION ALL
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, "tokenOut" AS token, -"amountOut" AS delta
        FROM balancer_v2."Vault_evt_Swap") swaps
        GROUP BY 1, 2, 3
    ),
    
    internal_changes AS (
        SELECT date_trunc('day', evt_block_time) AS day, '\xBA12222222228d8Ba445958a75a0704d566BF2C8'::bytea AS pool, token, SUM(COALESCE(delta, 0)) AS delta 
        FROM balancer_v2."Vault_evt_InternalBalanceChanged"
        GROUP BY 1, 2, 3
    ),
    
    balances_changes AS (
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, UNNEST(tokens) AS token, UNNEST(deltas) AS delta 
        FROM balancer_v2."Vault_evt_PoolBalanceChanged"
    ),
    
    managed_changes AS (
        SELECT date_trunc('day', evt_block_time) AS day, "poolId" AS pool, token, "managedDelta" AS delta
        FROM balancer_v2."Vault_evt_PoolBalanceManaged"
    ),
    
    daily_delta_balance AS (
        SELECT day, pool, token, SUM(COALESCE(amount, 0)) AS amount 
        FROM (
            SELECT day, pool, token, SUM(COALESCE(delta, 0)) AS amount 
            FROM balances_changes
            GROUP BY 1, 2, 3
            UNION ALL
            SELECT day, pool, token, delta AS amount 
            FROM swaps_changes
            UNION ALL
            SELECT day, pool, token, delta AS amount 
            FROM internal_changes
            UNION ALL
            SELECT day, pool, token, delta AS amount 
            FROM managed_changes
            ) b
        JOIN current_incentivized_pools p 
        ON p.pool_id = b.pool
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
    
    calendar AS (
        SELECT generate_series('2021-07-01'::timestamp, CURRENT_DATE, '1 day'::interval) AS day
    ),
    
    cumulative_usd_balance AS (
        SELECT c.day, b.pool, b.token, cumulative_amount,
        (p1.price * cumulative_amount / 10 ^ p1.decimals) AS amount_usd_from_api,
        0 AS amount_usd_from_dex
        FROM calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day AND c.day < b.day_of_next_change
        LEFT JOIN prices p1 ON p1.day = c.day AND p1.token = b.token
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
        SELECT day, pool, SUM(liquidity) AS tvl
        FROM estimated_pool_liquidity
        GROUP BY 1, 2
    ),
    
    timeframe_tvl AS (
        SELECT pool_id, AVG(tvl) AS avg_liquidity
        FROM total_tvl l
        JOIN current_incentivized_pools p 
        ON p.pool_id = l.pool
        AND l.day >= '{{1. Start date}}'
        AND l.day <= '{{2. End date}}'
        GROUP BY 1
    ),

    last_tvl AS(
        SELECT pool AS pool_id, t.tvl AS liquidity
        FROM total_tvl t
        WHERE day = date_trunc('day', now())
    )
    
SELECT 
    UPPER(name) AS name,
    SUBSTRING(created_at::text, 0, 11) AS created_at,
    bal_amount,
    all_time_rewards,
    last_90d_avg_rewards,
    timeframe_rewards,
    liquidity,
    avg_liquidity,
    all_time_fees,
    last_90d_fees,
    last_7d_fees,
    timeframe_fees,
    CONCAT('<a target="_blank" href="https://polygon.balancer.fi/#/pool/0', SUBSTRING(t.pool_id::text, 2), '">balancer ↗</a>') AS pool,
    CONCAT('<a target="_blank" href="https://duneanalytics.com/balancerlabs/Balancer-Pool-Analysis-on-Polygon?1.%20Pool%20ID=0', SUBSTRING(t.pool_id::text, 2), '">dune ↗</a>') AS analysis
FROM last_tvl t
LEFT JOIN timeframe_tvl t1
ON t1.pool_id = t.pool_id
LEFT JOIN all_time_rewards r1
ON r1.pool_id = t.pool_id
LEFT JOIN last_90d_avg_rewards r2
ON r2.pool_id = t.pool_id
LEFT JOIN timeframe_rewards r3
ON r3.pool_id = t.pool_id
LEFT JOIN all_time_fees f1
ON f1.pool_id = t.pool_id
LEFT JOIN last_90d_fees f2
ON f2.pool_id = t.pool_id
LEFT JOIN last_7d_fees f3
ON f3.pool_id = t.pool_id
LEFT JOIN timeframe_fees f4
ON f4.pool_id = t.pool_id
LEFT JOIN labels l
ON l.address = SUBSTRING(t.pool_id, 0, 21)
ORDER BY 2 DESC NULLS LAST