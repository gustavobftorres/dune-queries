-- part of a query repo
-- query name: reCLAMM Plasma Pool Balances Hourly
-- query link: https://dune.com/queries/5875552


WITH pool_tokens AS (
    SELECT 
        pool,
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[1]), '$.token'), 3)) as token_a, 
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[2]), '$.token'), 3)) as token_b 
    FROM balancer_v3_multichain.vault_evt_poolregistered where pool = {{pool}}
),
swap_increments AS (
    SELECT
        date_trunc('hour', S.evt_block_time + interval '1' hour) as "hour",
        CASE
            WHEN S.tokenIn = PT.token_a THEN (S.amountIn - S.swapFeeAmount/2)
            ELSE -S.amountOut
        END as increment_a,
        CASE
            WHEN S.tokenIn = PT.token_b THEN (S.amountIn  - S.swapFeeAmount/2)
            ELSE -S.amountOut
        END as increment_b
    FROM balancer_v3_multichain.vault_evt_swap S
    JOIN pool_tokens PT on PT.pool = S.pool
    WHERE S.pool = {{pool}} 
        AND S.evt_block_time >= TIMESTAMP '{{start}}' 
        AND S.evt_block_time <= now()
    
    UNION

    SELECT
        date_trunc('hour', LA.evt_block_time + interval '1' hour) as "hour",
        (LA.amountsAddedRaw[1] - LA.swapFeeAmountsRaw[1]/2) as increment_a,
        (LA.amountsAddedRaw[2] - LA.swapFeeAmountsRaw[2]/2) as increment_b
    FROM balancer_v3_multichain.vault_evt_liquidityadded LA
    WHERE LA.pool = {{pool}} 
        AND LA.evt_block_time >= TIMESTAMP '{{start}}' 
        AND LA.evt_block_time <= now()

    UNION 

    SELECT
        date_trunc('hour', LR.evt_block_time + interval '1' hour) as "hour",
        -(LR.amountsRemovedRaw[1] + LR.swapFeeAmountsRaw[1]/2) as increment_a,
        -(LR.amountsRemovedRaw[2] + LR.swapFeeAmountsRaw[2]/2) as increment_b
    FROM balancer_v3_multichain.vault_evt_liquidityremoved LR
    WHERE LR.pool = {{pool}} 
        AND LR.evt_block_time >= TIMESTAMP '{{start}}' 
        AND LR.evt_block_time <= now()
),
grouped_swap_increments as (
    SELECT 
        "hour", 
        SUM(increment_a) as increment_a, 
        SUM(increment_b) as increment_b 
    FROM swap_increments
    GROUP BY "hour"
),
token_balances as (
    SELECT
        "hour",
        SUM(increment_a) OVER (ORDER BY "hour" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS token_a_balance,
        SUM(increment_b) OVER (ORDER BY "hour" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS token_b_balance
    FROM grouped_swap_increments
)
SELECT
    "hour",
    CAST(token_a_balance as DOUBLE) / {{decimals_a}} as token_a_balance,
    CAST(token_b_balance as DOUBLE) / {{decimals_b}} as token_b_balance
FROM token_balances
ORDER BY "hour"
