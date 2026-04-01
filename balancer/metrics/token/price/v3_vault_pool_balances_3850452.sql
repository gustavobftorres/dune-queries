-- part of a query repo
-- query name: v3_vault_pool_balances
-- query link: https://dune.com/queries/3850452


WITH swaps_changes AS (
    SELECT
        day,
        pool_address,
        vault_address,
        token,
        SUM(COALESCE(delta, INT256 '0')) AS delta
    FROM
        (
            SELECT
                date_trunc('day', evt_block_time) AS day,
                contract_address AS vault_address,
                pool AS pool_address,
                tokenIn AS token,
                CAST(amountIn AS int256) AS delta
            FROM balancer_testnet_sepolia.Vault_evt_Swap

            UNION ALL

            SELECT
                date_trunc('day', evt_block_time) AS day,
                contract_address AS vault_address,
                pool AS pool_address,
                tokenOut AS token,
                -CAST(amountOut AS int256) AS delta
            FROM balancer_testnet_sepolia.Vault_evt_Swap
        ) swaps
    GROUP BY 1, 2, 3, 4
),

balances_changes AS (
    WITH token_data AS (
        SELECT
            pool,
            ARRAY_AGG(json_extract_scalar(token, '$.token') ORDER BY token_index) AS tokens  -- Cast to varbinary
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM
                balancer_testnet_sepolia.Vault_evt_PoolRegistered
        ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1 
    )
    SELECT
        date_trunc('day', evt_block_time) AS day,
        contract_address AS vault_address,
        pb.pool AS pool_address,
        t.token AS token,  -- Now a varbinary value
        d.delta
    FROM
        balancer_testnet_sepolia.Vault_evt_PoolBalanceChanged pb
        JOIN token_data td ON pb.pool = td.pool
        CROSS JOIN UNNEST(td.tokens) WITH ORDINALITY AS t(token, i)
        CROSS JOIN UNNEST(pb.deltas) WITH ORDINALITY AS d(delta, i)
    WHERE t.i = d.i
    ORDER BY 1, 2, 3
),

daily_delta_balance AS (
    SELECT
        day,
        vault_address,
        pool_address,
        token,
        SUM(COALESCE(amount, INT256 '0')) AS amount
    FROM
        (
            SELECT
                day,
                pool_address,
                vault_address,
                CAST(token AS VARCHAR) AS token,
                SUM(COALESCE(delta, INT256 '0')) AS amount
            FROM balances_changes
            GROUP BY 1, 2, 3, 4

            UNION ALL

            SELECT
                day,
                pool_address,
                vault_address,
                CAST(token AS VARCHAR) AS token, 
                delta AS amount
            FROM
                swaps_changes
        ) balance
    GROUP BY 1, 2, 3, 4
),

cumulative_balance AS (
    SELECT
        DAY,
        pool_address,
        vault_address,
        token,
        LEAD(DAY, 1, NOW()) OVER (PARTITION BY token, pool_address ORDER BY DAY) AS day_of_next_change,
        SUM(amount) OVER (PARTITION BY pool_address, token ORDER BY DAY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM daily_delta_balance
),

calendar AS (
    SELECT date_sequence AS day
    FROM unnest(sequence(date('2024-02-01'), date(now()), interval '1' day)) as t(date_sequence)
)

SELECT
    c.day,
    'sepolia' AS blockchain,
    b.vault_address,
    b.pool_address,
    b.token AS token_address,
    t.symbol AS token_symbol,
    cumulative_amount AS token_balance_raw,
    cumulative_amount / POWER(10, COALESCE(t.decimals, 18)) AS token_balance
FROM calendar c
LEFT JOIN cumulative_balance b ON b.day <= c.day
    AND c.day < b.day_of_next_change
LEFT JOIN tokens.erc20 t ON CAST(t.contract_address AS VARCHAR) = b.token 
    AND t.blockchain = 'sepolia'
WHERE b.pool_address IS NOT NULL
ORDER BY 1 DESC;