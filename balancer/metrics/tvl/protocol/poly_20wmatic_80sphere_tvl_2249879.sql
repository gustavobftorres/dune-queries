-- part of a query repo
-- query name: poly:20WMATIC-80SPHERE TVL
-- query link: https://dune.com/queries/2249879


WITH prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            AVG(price) AS price
        FROM prices.usd
        WHERE blockchain = "polygon"
        GROUP BY 1, 2
    ),

    dex_prices_1 AS (
        SELECT
            date_trunc('day', HOUR) AS DAY,
            contract_address AS token,
            percentile(median_price, 0.5) AS price,
            sum(sample_size) AS sample_size
        FROM dex.prices
        GROUP BY 1, 2
        HAVING sum(sample_size) > 3
    ),

    dex_prices AS (
        SELECT
            *,
            LEAD(DAY, 1, NOW()) OVER (
                PARTITION BY token
                ORDER BY
                    DAY
            ) AS day_of_next_change
        FROM
            dex_prices_1
    ),

    swaps_changes AS (
        SELECT
            day,
            pool_id,
            token,
            SUM(COALESCE(delta, 0)) AS delta
        FROM
            (
                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenIn AS token,
                    amountIn AS delta
                FROM
                    balancer_v2_polygon.Vault_evt_Swap
                WHERE poolId = '0xf3312968c7d768c19107731100ece7d4780b47b2000200000000000000000a50'
                UNION
                ALL
                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenOut AS token,
                    -amountOut AS delta
                FROM
                    balancer_v2_polygon.Vault_evt_Swap
                WHERE poolId = '0xf3312968c7d768c19107731100ece7d4780b47b2000200000000000000000a50'
                    
            ) swaps
        GROUP BY 1, 2, 3
    ),

zipped_balance_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            explode(arrays_zip(tokens, deltas, protocolFeeAmounts)) AS zipped
        FROM balancer_v2_polygon.Vault_evt_PoolBalanceChanged
        WHERE poolId = '0xf3312968c7d768c19107731100ece7d4780b47b2000200000000000000000a50'
    ),

    balances_changes AS (
        SELECT
            day,
            pool_id,
            zipped.tokens AS token,
            zipped.deltas - zipped.protocolFeeAmounts AS delta
        FROM zipped_balance_changes
        ORDER BY 1, 2, 3
    ),

    managed_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            token,
            cashDelta + managedDelta AS delta
        FROM balancer_v2_polygon.Vault_evt_PoolBalanceManaged
         WHERE poolId = '0xf3312968c7d768c19107731100ece7d4780b47b2000200000000000000000a50'

    ),

    daily_delta_balance AS (
        SELECT
            day,
            pool_id,
            token,
            SUM(COALESCE(amount, 0)) AS amount
        FROM
            (
                SELECT
                    day,
                    pool_id,
                    token,
                    SUM(COALESCE(delta, 0)) AS amount
                FROM
                    balances_changes
                GROUP BY 1, 2, 3
                UNION ALL
                SELECT
                    day,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    swaps_changes
                UNION ALL
                SELECT
                    day,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    managed_changes
            ) balance
        GROUP BY 1, 2, 3
    ),

    cumulative_balance AS (
        SELECT
            DAY,
            pool_id,
            token,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY token, pool_id ORDER BY DAY) AS day_of_next_change,
            SUM(amount) OVER (PARTITION BY pool_id, token ORDER BY DAY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
        FROM daily_delta_balance
    ),

    calendar AS (
        SELECT explode(sequence(to_date('2021-04-21'), CURRENT_DATE, interval 1 day)) AS day
    ),
    
   cumulative_usd_balance AS (
        SELECT
            c.day,
            b.pool_id,
            b.token,
            symbol AS token_symbol,
            cumulative_amount / POWER(10, t.decimals) * COALESCE(p1.price, p2.price, 0) AS amount_usd
        FROM calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day
        AND c.day < b.day_of_next_change
        LEFT JOIN prices.tokens t ON t.contract_address = b.token
        AND blockchain = "polygon"
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= c.day
        AND c.day < p2.day_of_next_change
        AND p2.token = b.token
        WHERE b.token != SUBSTRING(b.pool_id, 0, 42)
    ),
    
    pool_liquidity_estimates AS (
        SELECT
            b.day,
            b.pool_id,
            SUM(b.amount_usd) / COALESCE(SUM(w.normalized_weight), 1) AS liquidity
        FROM cumulative_usd_balance b
        LEFT JOIN balancer_v2_polygon.pools_tokens_weights w ON b.pool_id = w.pool_id
        AND b.token = w.token_address
        AND b.amount_usd > 0
        GROUP BY 1, 2
    )
    
SELECT
    b.day,
    b.pool_id,
    token AS token_address,
    token_symbol,
    coalesce(amount_usd, liquidity * normalized_weight) AS usd_amount
FROM pool_liquidity_estimates b
LEFT JOIN cumulative_usd_balance c ON c.day = b.day
AND c.pool_id = b.pool_id
LEFT JOIN balancer_v2_polygon.pools_tokens_weights w ON b.pool_id = w.pool_id
AND w.token_address = c.token
