-- part of a query repo
-- query name: Balancer Pools Created, by Version
-- query link: https://dune.com/queries/4119803


WITH pools AS (
    SELECT date_trunc('week', evt_block_time) as week, 'v1' as version, 'ethereum' AS chain, count(pool) as pools_registered
    FROM balancer_v1_ethereum.BFactory_evt_LOG_NEW_POOL
    GROUP BY 1, 2, 3
    
    UNION ALL
    
    SELECT date_trunc('week', evt_block_time) as week, 'v2' as version, chain, count(poolId) as pools_registered
    FROM balancer_v2_multichain.Vault_evt_PoolRegistered
    GROUP BY 1, 2, 3
    
    UNION ALL
    
    SELECT date_trunc('week', evt_block_time) as week, 'v3' as version, chain, count(pool) as pools_registered
    FROM balancer_v3_multichain.Vault_evt_PoolRegistered
    GROUP BY 1, 2, 3
    
    UNION ALL
    
    SELECT date_trunc('week', evt_block_time) as week, 'v1' as version, chain, count(bPool) as pools_registered
    FROM b_cow_amm_multichain.BCoWFactory_evt_LOG_NEW_POOL
    GROUP BY 1, 2, 3
)

SELECT
    week,
    version,
    pools_registered
FROM pools
WHERE week >= TIMESTAMP '{{1. Start date}}' 
AND ('{{3. Blockchain}}' = 'All' OR chain = '{{3. Blockchain}}')
