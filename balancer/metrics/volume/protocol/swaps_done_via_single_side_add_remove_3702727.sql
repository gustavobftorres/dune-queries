-- part of a query repo
-- query name: swaps done via single-side add/remove
-- query link: https://dune.com/queries/3702727


WITH ordered_swaps AS (
    SELECT
        *,
        bytearray_substring(poolId, 1, 20) AS poolAddress,
        ROW_NUMBER() OVER (PARTITION BY evt_tx_hash ORDER BY evt_index) AS swapIndex
    FROM balancer_v2_arbitrum.Vault_evt_Swap
    -- WHERE evt_block_time >= TIMESTAMP '2024-01-01'
)

SELECT DISTINCT t1.evt_tx_hash, t1.evt_block_time, t1.poolId
FROM ordered_swaps t1
JOIN ordered_swaps t2
ON t1.evt_tx_hash = t2.evt_tx_hash
AND t1.swapIndex = t2.swapIndex - 1
AND t1.tokenOut = t2.tokenIn
AND t1.poolId = t2.poolId
AND t1.tokenOut = t2.poolAddress
