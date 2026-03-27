-- part of a query repo
-- query name: reCLAMM_rebalancing_periods
-- query link: https://dune.com/queries/5756191


WITH virtual_balance_events AS (
    SELECT DISTINCT 
        evt_block_number,
        virtualBalanceA / 1e18 as virtual_balance_a,
        virtualBalanceB / 1e18 as virtual_balance_b
    FROM balancer_v3_multichain.reclammpool_evt_virtualbalancesupdated
    WHERE chain = '{{chain}}'
    AND contract_address = {{pool}}
),
swap_events AS (
    SELECT
        S.evt_index,
        S.evt_tx_hash,
        S.evt_block_number, 
        evt_block_time, 
        amountIn/ 1e18 as amount_in, 
        amountOut/ 1e18 as amount_out, 
        swapFeeAmount/ 1e18 as swap_fee_amount, 
        VB.virtual_balance_a,
        VB.virtual_balance_b,
        tokenIn as token_in, 
        tokenOut as token_out,
        CASE 
            -- If the next swap rebalances the pool, is because the current swap put the pool in rebalance mode
            WHEN (lead(VB.evt_block_number) over (ORDER BY S.evt_block_number)) IS NOT NULL AND VB.evt_block_number IS NULL THEN 1
            WHEN VB.evt_block_number IS NOT NULL THEN 1 
            ELSE 0
        END AS "is_rebalancing"
    FROM balancer_v3_multichain.vault_evt_swap as S
    LEFT JOIN virtual_balance_events as VB ON VB.evt_block_number = S.evt_block_number
    WHERE chain = '{{chain}}'
    AND pool = {{pool}}
    ORDER BY evt_block_time
),
rebalancing_periods as (
    SELECT 
        evt_tx_hash,
        evt_block_number,
        CASE 
            WHEN LAG(is_rebalancing) OVER (ORDER BY evt_block_time) != is_rebalancing THEN 1 
            ELSE 0 
        END AS rebalance_state_change
    FROM swap_events
    ORDER BY evt_block_time
),
period_ids AS (
    SELECT
        evt_tx_hash,
        evt_block_number,
        SUM(rebalance_state_change) OVER (ORDER BY evt_block_number ROWS UNBOUNDED PRECEDING) + 1 AS period_id
    FROM rebalancing_periods
    ORDER BY evt_block_number
),
trades AS (
    SELECT * FROM dex.trades WHERE blockchain = '{{chain}}' AND project = 'balancer'
)
SELECT
    MIN(evt_block_time) as rebalance_started_at,
    MAX(evt_block_time) as rebalance_ended_at,
    count(*) as num_swaps,
    SUM(T.amount_usd) as usd_volume
FROM swap_events AS S
JOIN period_ids AS P ON P.evt_tx_hash = S.evt_tx_hash
JOIN trades T on T.tx_hash = P.evt_tx_hash AND T.evt_index = S.evt_index
WHERE is_rebalancing = 1
GROUP BY period_id
ORDER BY period_id
