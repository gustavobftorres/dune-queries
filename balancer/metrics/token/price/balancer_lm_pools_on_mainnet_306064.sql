-- part of a query repo
-- query name: Balancer LM Pools on Mainnet
-- query link: https://dune.com/queries/306064


WITH labels AS (
        SELECT * 
        FROM (SELECT
            address,
            name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.labels
        WHERE "type" = 'balancer_v2_pool'
        GROUP BY 1, 2) l
        WHERE num = 1
    ),
    
    weekly_rewards AS (
        SELECT date_trunc('week', day) AS week, pool_id, SUM(amount) AS bal_amount
        FROM dune_user_generated.balancer_liquidity_mining
        WHERE chain_id = '1'
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
        SELECT pool_id, SUM(amount)::int AS all_time_rewards
        FROM dune_user_generated.balancer_liquidity_mining
        WHERE chain_id = '1'
        GROUP BY 1
    ),
    
    timeframe_rewards AS (
        SELECT pool_id, SUM(amount)::int AS timeframe_rewards
        FROM dune_user_generated.balancer_liquidity_mining
        WHERE chain_id = '1'
        AND day >= '{{1. Start date}}'
        AND day <= '{{2. End date}}'
        GROUP BY 1
    ),
    
    last_90d_fees AS (
        SELECT pool_id, SUM(usd_amount * swap_fee) AS last_90d_fees
        FROM balancer.view_trades t
        JOIN current_incentivized_pools p 
        ON p.pool_id = t.exchange_contract_address
        AND t.block_time >= CURRENT_DATE - '90d'::interval
        GROUP BY 1
    ),
    
    last_7d_fees AS (
        SELECT pool_id, SUM(usd_amount * swap_fee) AS last_7d_fees
        FROM balancer.view_trades t
        JOIN current_incentivized_pools p 
        ON p.pool_id = t.exchange_contract_address
        AND t.block_time >= CURRENT_DATE - '7d'::interval
        GROUP BY 1
    ),
    
    all_time_fees AS (
        SELECT pool_id, SUM(usd_amount * swap_fee) AS all_time_fees
        FROM balancer.view_trades t
        JOIN current_incentivized_pools p 
        ON p.pool_id = t.exchange_contract_address
        GROUP BY 1
    ),
    
    timeframe_fees AS (
        SELECT pool_id, SUM(usd_amount * swap_fee) AS timeframe_fees
        FROM balancer.view_trades t
        JOIN current_incentivized_pools p 
        ON p.pool_id = t.exchange_contract_address
        AND block_time >= '{{1. Start date}}'
        AND block_time <= '{{2. End date}}'
        GROUP BY 1
    ),
    
    tvl AS (
        SELECT pool_id, created_at, liquidity, bal_amount::int
        FROM balancer.view_pools_liquidity l
        JOIN current_incentivized_pools p 
        ON p.pool_id = l.pool
        AND l.day = CURRENT_DATE
    ),
    
    timeframe_tvl AS (
        SELECT pool_id, AVG(liquidity) AS avg_liquidity
        FROM balancer.view_pools_liquidity l
        JOIN current_incentivized_pools p 
        ON p.pool_id = l.pool
        AND l.day >= '{{1. Start date}}'
        AND l.day <= '{{2. End date}}'
        GROUP BY 1
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
    CONCAT('<a target="_blank" href="https://app.balancer.fi/#/pool/0', SUBSTRING(t.pool_id::text, 2), '">balancer ↗</a>') AS pool,
    CONCAT('<a target="_blank" href="https://duneanalytics.com/balancerlabs/Balancer-Pool-Analysis?1.%20Pool%20ID=0', SUBSTRING(t.pool_id::text, 2), '">dune ↗</a>') AS analysis
FROM tvl t
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