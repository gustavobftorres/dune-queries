-- part of a query repo
-- query name: Balancer Pools Created, by Blockchain
-- query link: https://dune.com/queries/3548066


WITH pools AS (
    SELECT date_trunc('week', evt_block_time) as week, '1' as version, 'ethereum' AS chain, count(pool) as pools_registered
    FROM balancer_v1_ethereum.BFactory_evt_LOG_NEW_POOL
    GROUP BY 1, 2, 3
    
    UNION ALL
    
    SELECT date_trunc('week', evt_block_time) as week, '2' as version, chain, count(poolId) as pools_registered
    FROM balancer_v2_multichain.Vault_evt_PoolRegistered
    GROUP BY 1, 2, 3
    
    UNION ALL
    
    SELECT date_trunc('week', evt_block_time) as week, '3' as version, chain, count(pool) as pools_registered
    FROM balancer_v3_multichain.Vault_evt_PoolRegistered
    GROUP BY 1, 2, 3
    
    UNION ALL
    
    SELECT date_trunc('week', evt_block_time) as week, 'CoW' as version, chain, count(bPool) as pools_registered
    FROM b_cow_amm_multichain.BCoWFactory_evt_LOG_NEW_POOL
    GROUP BY 1, 2, 3
)

SELECT 
    week,
    pools_registered,
     chain || 
        CASE 
            WHEN chain = 'arbitrum' THEN ' 🟦'
            WHEN chain = 'avalanche_c' THEN ' ⬜ '
            WHEN chain = 'base' THEN ' 🟨'
            WHEN chain = 'ethereum' THEN ' Ξ'
            WHEN chain = 'gnosis' THEN ' 🟩'
            WHEN chain = 'optimism' THEN ' 🔴'
            WHEN chain = 'polygon' THEN ' 🟪'
            WHEN chain = 'zkevm' THEN ' 🟣'
        END 
    AS blockchain, '2' AS version 
FROM pools
WHERE week >= TIMESTAMP '{{1. Start date}}' 
AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')

