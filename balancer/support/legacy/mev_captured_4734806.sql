-- part of a query repo
-- query name: MEV captured
-- query link: https://dune.com/queries/4734806


WITH swap_fees AS (
    SELECT * FROM (
        SELECT
            swaps.chain,
            swaps.pool,
            swaps.evt_tx_hash,
            swaps.evt_index,
            swaps.evt_block_number,
            fees.swap_fee_percentage / POWER(10,18) AS swap_fee_percentage
        FROM balancer_v3_multichain.vault_evt_swap swaps
        JOIN query_4761874 q ON swaps.pool = q.pool AND swaps.chain = q.chain AND q.hook_name = 'mev_capture'
        LEFT JOIN balancer.pools_fees fees
            ON fees.contract_address = bytearray_substring(swaps.pool, 1, 20)
            AND ARRAY[fees.block_number] || ARRAY[fees.index] < ARRAY[swaps.evt_block_number] || ARRAY[swaps.evt_index]
            AND fees.version = '3'
            AND swaps.chain = fees.blockchain
    ) t
)

SELECT 
    t.*,
    f.swap_fee_percentage AS expected_swap_fee,
    t.swap_fee AS actual_swap_fee,
    t.swap_fee - f.swap_fee_percentage AS fee_delta,
    (t.swap_fee - f.swap_fee_percentage) * amount_usd AS mev_captured
FROM swap_fees f
JOIN balancer.trades t ON t.version = '3'
AND t.blockchain = f.chain
AND f.evt_block_number = t.block_number
AND t.pool_id = f.pool
AND f.evt_tx_hash = t.tx_hash