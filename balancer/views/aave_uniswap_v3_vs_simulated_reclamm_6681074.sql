-- part of a query repo
-- query name: AAVE Uniswap v3 vs Simulated reCLAMM
-- query link: https://dune.com/queries/6681074


WITH pool_tokens AS (
    SELECT 
        pool,
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[1]), '$.token'), 3)) as token_a, 
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[2]), '$.token'), 3)) as token_b 
    FROM balancer_v3_ethereum.vault_evt_poolregistered 
    WHERE pool = 0x9d1Fcf346eA1b073de4D5834e25572CC6ad71f4d
),
swaps_in AS (
    SELECT
        evt_block_number,
        evt_block_time,
        pool,
        tokenIn as token,
        CAST(amountIn AS DOUBLE) - CAST(swapFeeAmount AS DOUBLE) * CASE 
            WHEN evt_block_number >= 24033315 THEN 0.25
            ELSE 0.50
        END as delta
    FROM balancer_v3_ethereum.vault_evt_swap
    WHERE pool = 0x9d1Fcf346eA1b073de4D5834e25572CC6ad71f4d
),
swaps_out AS (
    SELECT
        evt_block_number,
        evt_block_time,
        pool,
        tokenOut as token,
        -CAST(amountOut AS DOUBLE) as delta
    FROM balancer_v3_ethereum.vault_evt_swap
    WHERE pool = 0x9d1Fcf346eA1b073de4D5834e25572CC6ad71f4d
),
liquidity_added AS (
    SELECT
        evt_block_number,
        evt_block_time,
        LA.pool,
        PT.token_a as token,
        CAST(amountsAddedRaw[1] AS DOUBLE) - CAST(swapFeeAmountsRaw[1] AS DOUBLE) * CASE 
            WHEN evt_block_number >= 24033315 THEN 0.25
            ELSE 0.50
        END as delta
    FROM balancer_v3_ethereum.vault_evt_liquidityadded LA
    CROSS JOIN pool_tokens PT
    WHERE LA.pool = 0x9d1Fcf346eA1b073de4D5834e25572CC6ad71f4d
    
    UNION ALL
    
    SELECT
        evt_block_number,
        evt_block_time,
        LA.pool,
        PT.token_b as token,
        CAST(amountsAddedRaw[2] AS DOUBLE) - CAST(swapFeeAmountsRaw[2] AS DOUBLE) * CASE 
            WHEN evt_block_number >= 24033315 THEN 0.25
            ELSE 0.50
        END as delta
    FROM balancer_v3_ethereum.vault_evt_liquidityadded LA
    CROSS JOIN pool_tokens PT
    WHERE LA.pool = 0x9d1Fcf346eA1b073de4D5834e25572CC6ad71f4d
),
liquidity_removed AS (
    SELECT
        evt_block_number,
        evt_block_time,
        LR.pool,
        PT.token_a as token,
        -CAST(amountsRemovedRaw[1] AS DOUBLE) + CAST(swapFeeAmountsRaw[1] AS DOUBLE) * CASE 
            WHEN evt_block_number >= 24033315 THEN 0.25
            ELSE 0.50
        END as delta
    FROM balancer_v3_ethereum.vault_evt_liquidityremoved LR
    CROSS JOIN pool_tokens PT
    WHERE LR.pool = 0x9d1Fcf346eA1b073de4D5834e25572CC6ad71f4d
    
    UNION ALL
    
    SELECT
        evt_block_number,
        evt_block_time,
        LR.pool,
        PT.token_b as token,
        -CAST(amountsRemovedRaw[2] AS DOUBLE) + CAST(swapFeeAmountsRaw[2] AS DOUBLE) * CASE 
            WHEN evt_block_number >= 24033315 THEN 0.25
            ELSE 0.50
        END as delta
    FROM balancer_v3_ethereum.vault_evt_liquidityremoved LR
    CROSS JOIN pool_tokens PT
    WHERE LR.pool = 0x9d1Fcf346eA1b073de4D5834e25572CC6ad71f4d
),
all_changes AS (
    SELECT * FROM swaps_in
    UNION ALL
    SELECT * FROM swaps_out
    UNION ALL
    SELECT * FROM liquidity_added
    UNION ALL
    SELECT * FROM liquidity_removed
),
token_balances AS (
    SELECT
        evt_block_number as block_number,
        evt_block_time as block_time,
        token,
        SUM(delta) OVER (PARTITION BY token ORDER BY evt_block_number, evt_block_time) as token_balance
    FROM all_changes
),
virtual_balances AS (
    SELECT 
        VBU.evt_block_number as block_number,
        VBU.evt_block_time as block_time,
        MAX(virtualBalanceA) as virtual_balance_a,
        MAX(virtualBalanceB) as virtual_balance_b 
    FROM balancer_v3_ethereum.reclammpool_evt_virtualbalancesupdated VBU
    WHERE contract_address = 0x9d1Fcf346eA1b073de4D5834e25572CC6ad71f4d
    GROUP BY VBU.evt_block_number, VBU.evt_block_time
),
combined_data AS (
    SELECT 
        COALESCE(VB.block_number, TB.block_number) as block_number,
        COALESCE(VB.block_time, TB.block_time) as block_time,
        VB.virtual_balance_a,
        VB.virtual_balance_b,
        TB.token,
        TB.token_balance
    FROM virtual_balances VB
    FULL OUTER JOIN token_balances TB 
        ON VB.block_number = TB.block_number
),
combined_flagged AS (
    SELECT *,
        SUM(CASE WHEN virtual_balance_a IS NOT NULL THEN 1 ELSE 0 END) 
            OVER (ORDER BY block_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS vb_grp,
        SUM(CASE WHEN token_balance IS NOT NULL THEN 1 ELSE 0 END) 
            OVER (PARTITION BY token ORDER BY block_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS tb_grp
    FROM combined_data
),
reclamm_balances_filled AS (
    SELECT 
        block_number,
        block_time,
        token,
        MAX(virtual_balance_a) OVER (PARTITION BY vb_grp ORDER BY block_number) AS virtual_balance_a,
        MAX(virtual_balance_b) OVER (PARTITION BY vb_grp ORDER BY block_number) AS virtual_balance_b,
        MAX(token_balance) OVER (PARTITION BY token, tb_grp ORDER BY block_number) AS token_balance
    FROM combined_flagged
),
reclamm_balances_unpivoted AS (
    SELECT 
        block_number,
        block_time,
        token,
        CASE 
            WHEN token = PT.token_a THEN virtual_balance_a
            ELSE virtual_balance_b
        END as virtual_balance,
        token_balance
    FROM reclamm_balances_filled
    CROSS JOIN pool_tokens PT
    WHERE token IN (PT.token_a, PT.token_b)
),
uniswap_trades AS (
    SELECT 
        block_number,
        block_time,
        token_bought_address,
        token_bought_symbol,
        token_sold_address,
        token_sold_symbol,
        token_bought_amount_raw,
        token_sold_amount_raw,
        amount_usd,
        tx_hash,
        tx_to,
        evt_index
    FROM dex.trades
    WHERE blockchain = 'ethereum'
        AND project_contract_address = 0x5aB53EE1d50eeF2C1DD3d5402789cd27bB52c1bB
        AND block_time >= TIMESTAMP '2026-02-01'
),
trades_and_balances AS (
    SELECT 
        T.block_number as trade_block,
        T.block_time as trade_time,
        T.token_bought_address,
        T.token_bought_symbol,
        T.token_sold_address,
        T.token_sold_symbol,
        T.token_bought_amount_raw as uni_amount_out_raw,
        T.token_sold_amount_raw as uni_amount_in_raw,
        T.amount_usd,
        T.tx_hash,
        T.tx_to,
        T.evt_index,
        RB.block_number as balance_block,
        RB.token as token_address,
        RB.token_balance,
        RB.virtual_balance
    FROM uniswap_trades T
    INNER JOIN reclamm_balances_unpivoted RB
        ON RB.block_number <= T.block_number
        AND (RB.token = T.token_sold_address OR RB.token = T.token_bought_address)
),
latest_balances_per_trade AS (
    SELECT 
        trade_block,
        trade_time,
        token_bought_address,
        token_bought_symbol,
        token_sold_address,
        token_sold_symbol,
        uni_amount_out_raw,
        uni_amount_in_raw,
        amount_usd,
        tx_hash,
        tx_to,
        evt_index,
        token_address,
        token_balance,
        virtual_balance,
        ROW_NUMBER() OVER (PARTITION BY trade_block, evt_index, token_address ORDER BY balance_block DESC) as rn
    FROM trades_and_balances
),
latest_balances_filtered AS (
    SELECT *
    FROM latest_balances_per_trade
    WHERE rn = 1
),
trades_with_balances AS (
    SELECT 
        LB_IN.trade_block as block_number,
        LB_IN.trade_time as block_time,
        LB_IN.token_sold_address,
        LB_IN.token_sold_symbol,
        LB_IN.token_bought_address,
        LB_IN.token_bought_symbol,
        LB_IN.uni_amount_in_raw,
        LB_IN.uni_amount_out_raw,
        LB_IN.amount_usd,
        LB_IN.tx_hash,
        LB_IN.tx_to,
        LB_IN.evt_index,
        LB_IN.token_balance as balance_token_in,
        LB_IN.virtual_balance as virtual_balance_token_in,
        LB_OUT.token_balance as balance_token_out,
        LB_OUT.virtual_balance as virtual_balance_token_out
    FROM latest_balances_filtered LB_IN
    INNER JOIN latest_balances_filtered LB_OUT
        ON LB_IN.trade_block = LB_OUT.trade_block
        AND LB_IN.evt_index = LB_OUT.evt_index
        AND LB_IN.token_address = LB_IN.token_sold_address
        AND LB_OUT.token_address = LB_OUT.token_bought_address
)
SELECT 
    block_number,
    block_time,
    token_sold_address,
    token_sold_symbol as token_in_symbol,
    token_bought_address,
    token_bought_symbol as token_out_symbol,
    uni_amount_in_raw,
    uni_amount_out_raw,
    (balance_token_out + virtual_balance_token_out) * 
    (1 - (balance_token_in + virtual_balance_token_in) / (balance_token_in + virtual_balance_token_in + CAST(uni_amount_in_raw AS DOUBLE) * 0.9975)) 
    as bal_amount_out_raw,
    (balance_token_out * 1.1 + virtual_balance_token_out * 1.1) * 
    (1 - (balance_token_in * 1.1 + virtual_balance_token_in * 1.1) / (balance_token_in * 1.1 + virtual_balance_token_in * 1.1 + CAST(uni_amount_in_raw AS DOUBLE) * 0.9975)) 
    as bal_amount_out_raw_10pct,
    (balance_token_out * 1.25 + virtual_balance_token_out * 1.25) * 
    (1 - (balance_token_in * 1.25 + virtual_balance_token_in * 1.25) / (balance_token_in * 1.25 + virtual_balance_token_in * 1.25 + CAST(uni_amount_in_raw AS DOUBLE) * 0.9975)) 
    as bal_amount_out_raw_25pct,
    (balance_token_out * 1.5 + virtual_balance_token_out * 1.5) * 
    (1 - (balance_token_in * 1.5 + virtual_balance_token_in * 1.5) / (balance_token_in * 1.5 + virtual_balance_token_in * 1.5 + CAST(uni_amount_in_raw AS DOUBLE) * 0.9975)) 
    as bal_amount_out_raw_50pct,
    (balance_token_out * 2.0 + virtual_balance_token_out * 2.0) * 
    (1 - (balance_token_in * 2.0 + virtual_balance_token_in * 2.0) / (balance_token_in * 2.0 + virtual_balance_token_in * 2.0 + CAST(uni_amount_in_raw AS DOUBLE) * 0.9975)) 
    as bal_amount_out_raw_100pct,
    amount_usd,
    tx_hash,
    tx_to,
    evt_index,
    balance_token_in,
    virtual_balance_token_in,
    balance_token_out,
    virtual_balance_token_out
FROM trades_with_balances
ORDER BY block_number, evt_index
