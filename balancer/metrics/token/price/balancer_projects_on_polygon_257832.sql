-- part of a query repo
-- query name: Balancer Projects on Polygon
-- query link: https://dune.com/queries/257832


WITH projects AS (
        SELECT 'trueusd' AS name, '\x0d34e5dd4d8f043557145598e4e2dc286b35fd4f'::bytea AS address
        UNION ALL
        SELECT 'dhedge' AS name, '\x5028497af0c9a54ea8c6d42a054c0341b9fc6168'::bytea AS address
        -- FROM labels.labels
        -- WHERE "type" = 'balancer_project'
        -- AND author IN ('balancerlabs', 'metacrypto', 'markusbkoch', 'mangool', 'astivelman')
    ),
    
    last_30d_volume AS (
        SELECT 
            p.name,
            SUM(usd_amount) AS last_30d_volume
        FROM dex.trades d
        JOIN projects p 
        ON p.address = SUBSTRING(exchange_contract_address, 0, 21)
        AND d.project = 'Balancer'
        AND block_time >= CURRENT_DATE - interval '30d'
        GROUP BY 1
    ),
    
    weekly_volume AS (
        SELECT 
            p.name,
            date_trunc('week', block_time) AS week,
            SUM(usd_amount) AS volume
        FROM dex.trades d
        JOIN projects p 
        ON p.address = SUBSTRING(exchange_contract_address, 0, 21)
        AND d.project = 'Balancer'
        AND block_time >= '{{1. Start date}}'
        AND block_time <= '{{2. End date}}'
        GROUP BY 1, 2
    ),
    
    volume_stats AS (
        SELECT
            name,
            SUM(volume) AS volume,
            AVG(volume) AS avg_volume
        FROM weekly_volume
        GROUP BY 1
    ),
    
    prices AS (
        SELECT date_trunc('day', minute) AS day, contract_address AS token, decimals, AVG(price) AS price
        FROM prices.usd
        WHERE minute >= '{{1. Start date}}'
        AND minute <= '{{2. End date}}'
        GROUP BY 1, 2, 3
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
        JOIN projects p
        ON p.address = SUBSTRING(pool, 0, 21)
        AND day <= '{{2. End date}}'
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
    
    weekly_delta_balance_by_token AS (
        SELECT day, pool, token, cumulative_amount, (cumulative_amount - COALESCE(LAG(cumulative_amount, 1) OVER (PARTITION BY pool, token ORDER BY day), 0)) AS amount
        FROM (SELECT day, pool, token, SUM(cumulative_amount) AS cumulative_amount
        FROM cumulative_balance b
        WHERE extract(dow from day) = 1
        GROUP BY 1, 2, 3) foo
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

    tvl_stats AS (
        SELECT name, SUM(liquidity) AS tvl
        FROM estimated_pool_liquidity
        JOIN projects p
        ON p.address = SUBSTRING(pool, 0, 21)
        AND day = CURRENT_DATE
        GROUP BY 1
    )

SELECT v.name, tvl, volume, last_30d_volume, avg_volume
FROM volume_stats v
JOIN tvl_stats t
ON v.name = t.name
JOIN last_30d_volume l
ON v.name = l.name
