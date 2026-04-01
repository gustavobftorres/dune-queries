-- part of a query repo
-- query name: Invariant/Balances Growth
-- query link: https://dune.com/queries/4175112


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
),

invariant_data AS (
    SELECT 
        call_block_number,
        call_block_date,
        call_block_time,
        call_tx_index,
        call_tx_hash,
        call_trace_address,
        contract_address AS pool_address,
        output_0 AS current_invariant,
        balancesLiveScaled18,
        rounding AS rounding,
        ROW_NUMBER() OVER (PARTITION BY contract_address, call_block_number ORDER BY call_block_number ASC, call_tx_index DESC, call_trace_address ASC) AS rn,
        COALESCE(LAG(output_0) OVER (PARTITION BY contract_address ORDER BY call_block_number ASC, call_tx_index DESC, call_trace_address ASC), 0) AS previous_invariant,
        COALESCE(LAG(call_block_number) OVER (PARTITION BY contract_address ORDER BY call_block_number ASC, call_tx_index DESC, call_trace_address ASC), call_block_number) AS previous_block_number,
        COALESCE(LEAD(call_block_number) OVER (PARTITION BY contract_address ORDER BY call_block_number ASC, call_tx_index DESC, call_trace_address ASC), call_block_number) AS next_block_number
    FROM balancer_testnet_sepolia.StablePool_call_computeInvariant
    WHERE 1 = 1
    AND call_success
    ORDER BY call_block_number DESC, call_tx_index DESC, call_trace_address DESC
),

aggregate AS (
SELECT 
    i.call_block_number,
    i.call_block_date,
    i.call_block_time,
    i.call_tx_index,
    i.call_tx_hash,
    i.pool_address,
    t.token AS token_address,
    i.current_invariant,
    i.previous_invariant,
    CASE 
        WHEN i.previous_invariant IS NOT NULL AND i.current_invariant >= i.previous_invariant
        THEN i.current_invariant - i.previous_invariant
        ELSE i.current_invariant
    END AS invariant_growth,
    d.delta AS balancesLiveScaled18, 
    i.previous_block_number AS block_of_last_change,
    i.next_block_number AS block_of_next_change,
    rn
FROM
    invariant_data i
    JOIN token_data td ON i.pool_address = td.pool
    CROSS JOIN UNNEST(td.tokens) WITH ORDINALITY AS t(token, i)  
    CROSS JOIN UNNEST(i.balancesLiveScaled18) WITH ORDINALITY AS d(delta, i)
WHERE 1 = 1
AND t.i = d.i),

swaps AS
(
    SELECT
    evt_block_number,
    pool,
    tokenIn,
    tokenOut,
    CASE WHEN tokenIn = 0x8a88124522dbbf1e56352ba3de1d9f78c143751e --USDC
    THEN swapFeeAmount * POWER(10,12)
    ELSE swapFeeAmount
    END as swapFeeAmountScaled18,
    CASE WHEN tokenIn = 0x8a88124522dbbf1e56352ba3de1d9f78c143751e --USDC
    THEN amountIn * POWER(10,12)
    ELSE amountIn
    END AS amountIn,
    CASE WHEN tokenOut = 0x8a88124522dbbf1e56352ba3de1d9f78c143751e --USDC
    THEN amountOut * POWER(10,12)
    ELSE amountOut
    END AS amountOut
    FROM balancer_testnet_sepolia.Vault_evt_Swap
)

SELECT 
    call_block_number,
    block_of_next_change,
    block_of_last_change,
    call_block_date,
    call_block_time,
    call_tx_index,
    call_tx_hash,
    rn,
    pool_address,
    token_address,
    current_invariant,
    previous_invariant,
    invariant_growth,
    balancesLiveScaled18, 
    COALESCE(LAG(balancesLiveScaled18) OVER (PARTITION BY pool_address, token_address ORDER BY call_block_number ASC, rn ASC), 0) AS previous_balance,
    SUM(CASE WHEN a.token_address = CAST(s.tokenIn AS VARCHAR)
    THEN amountIn
    WHEN a.token_address = CAST(s.tokenOut AS VARCHAR)
    THEN - amountOut END) AS swap_balance_change,
    SUM(CASE WHEN a.token_address = CAST(s.tokenIn AS VARCHAR)
    THEN swapFeeAmountScaled18
    ELSE 0 END) AS swapFeeCollected
FROM aggregate a
LEFT JOIN swaps s ON a.pool_address = s.pool
AND(CAST(s.tokenIn AS VARCHAR) IN (a.token_address) OR CAST(s.tokenOut AS VARCHAR) IN (a.token_address))
AND (s.evt_block_number > block_of_last_change AND s.evt_block_number < block_of_next_change
AND s.evt_block_number < call_block_number)
AND a.rn = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
ORDER BY call_block_number ASC, call_tx_index DESC, rn ASC, pool_address, token_address