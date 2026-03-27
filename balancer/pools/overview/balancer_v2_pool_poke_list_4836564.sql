-- part of a query repo
-- query name: Balancer v2 Pool Poke List
-- query link: https://dune.com/queries/4836564


WITH all_events AS (
    SELECT 
        chain,
        poolId,
        MAX(evt_block_time) AS evt_block_time
    FROM balancer_v2_multichain.vault_evt_poolbalancechanged
    GROUP BY 1, 2
    
    UNION ALL
    
    SELECT 
        chain,
        poolId,
        MAX(evt_block_time)
    FROM balancer_v2_multichain.vault_evt_swap
    WHERE tokenIn = BYTEARRAY_SUBSTRING(poolId, 1, 20)
    OR tokenOut = BYTEARRAY_SUBSTRING(poolId, 1, 20)
    GROUP BY 1, 2
),

latest_events AS (
    SELECT 
        chain,
        poolId,
        MAX(evt_block_time) as latest_add_remove
    FROM all_events
    GROUP BY 1, 2
),

trades_after_last_add_remove AS (
    SELECT 
        le.chain,
        le.poolId,
        le.latest_add_remove,
        SUM(t.amount_usd * t.swap_fee) as fees_since_last_add_remove
    FROM balancer.trades t
    JOIN latest_events le
    ON le.chain = t.blockchain 
    AND le.poolId = t.pool_id
    AND t.block_date > le.latest_add_remove
    GROUP BY 1, 2, 3
)

SELECT 
    chain,
    poolId,
    name AS symbol,
    latest_add_remove,
    COALESCE(fees_since_last_add_remove, 0) as fees_since_last_add_remove
FROM trades_after_last_add_remove t
LEFT JOIN labels.balancer_v2_pools l
ON l.address = BYTEARRAY_SUBSTRING(t.poolId, 1, 20)
AND l.blockchain = t.chain
WHERE latest_add_remove < CURRENT_DATE - INTERVAL '12' DAY
ORDER BY 5 DESC
