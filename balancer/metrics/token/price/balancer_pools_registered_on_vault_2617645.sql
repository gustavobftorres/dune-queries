-- part of a query repo
-- query name: Balancer Pools Registered on Vault
-- query link: https://dune.com/queries/2617645


WITH
arb_pools as(
SELECT 
DISTINCT poolAddress as "Pool Address",
UPPER(name) as "Pool Name",
evt_block_time as "Pool Registered @",
'arbitrum' as blockchain,
sum (pool_liquidity_usd) as TVL
FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered v
LEFT JOIN balancer_v2_arbitrum.liquidity l
ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id,1,42)
LEFT JOIN labels.addresses a
ON v.poolAddress = a.address
WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') and l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY and 
name NOT LIKE '%?%'
GROUP BY 1,2,3),

eth_pools as(
SELECT 
DISTINCT poolAddress as "Pool Address",
UPPER(name) as "Pool Name",
evt_block_time as "Pool Registered @",
'ethereum' as blockchain,
sum (pool_liquidity_usd) as TVL
FROM balancer_v2_ethereum.Vault_evt_PoolRegistered v
LEFT JOIN balancer_v2_ethereum.liquidity l
ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id,1,42)
LEFT JOIN labels.addresses a
ON v.poolAddress = a.address
WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') and l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY and 
name NOT LIKE '%?%'
GROUP BY 1,2,3
),

gno_pools as(
SELECT 
DISTINCT poolAddress as "Pool Address",
UPPER(name) as "Pool Name",
evt_block_time as "Pool Registered @",
'gnosis' as blockchain,
sum (pool_liquidity_usd) as TVL
FROM balancer_v2_gnosis.Vault_evt_PoolRegistered v
LEFT JOIN balancer_v2_gnosis.liquidity l
ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id,1,42)
LEFT JOIN labels.addresses a
ON v.poolAddress = a.address
WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') and l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY and 
name NOT LIKE '%?%'
GROUP BY 1,2,3),

opt_pools as(
SELECT 
DISTINCT poolAddress as "Pool Address",
UPPER(name) as "Pool Name",
evt_block_time as "Pool Registered @",
'optimism' as blockchain,
sum (pool_liquidity_usd) as TVL
FROM balancer_v2_optimism.Vault_evt_PoolRegistered v
LEFT JOIN balancer_v2_optimism.liquidity l
ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id,1,42)
LEFT JOIN labels.addresses a
ON v.poolAddress = a.address
WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') and l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY and 
name NOT LIKE '%?%'
GROUP BY 1,2,3),

pol_pools as(
SELECT 
DISTINCT poolAddress as "Pool Address",
UPPER(name) as "Pool Name",
evt_block_time as "Pool Registered @",
'polygon' as blockchain,
sum (pool_liquidity_usd) as TVL
FROM balancer_v2_polygon.Vault_evt_PoolRegistered v
LEFT JOIN balancer_v2_polygon.liquidity l
ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id,1,42)
LEFT JOIN labels.addresses a
ON v.poolAddress = a.address
WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') and l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY and 
name NOT LIKE '%?%'
GROUP BY 1,2,3),

zkevm_pools as(
SELECT 
DISTINCT poolAddress as "Pool Address",
UPPER(name) as "Pool Name",
evt_block_time as "Pool Registered @",
'zkevm' as blockchain,
sum (pool_liquidity_usd) as TVL
FROM balancer_v2_zkevm.Vault_evt_PoolRegistered v
LEFT JOIN balancer.liquidity l
ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id,1,42)
AND l.blockchain = 'zkevm' AND l.version = '2'
LEFT JOIN labels.addresses a
ON v.poolAddress = a.address
WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') and l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY and 
name NOT LIKE '%?%'
GROUP BY 1,2,3),

all as (
SELECT * FROM arb_pools
UNION ALL
SELECT * FROM eth_pools
UNION ALL
SELECT * FROM opt_pools
UNION ALL
SELECT * FROM pol_pools
UNION ALL
SELECT * FROM gno_pools
UNION ALL
SELECT * FROM zkevm_pools)

SELECT * from all
WHERE ('{{4. Blockchain}}' = 'All' or blockchain = '{{4. Blockchain}}')
ORDER BY 3 DESC