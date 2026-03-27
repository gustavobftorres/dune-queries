-- part of a query repo
-- query name: (query_2634572) pool_data
-- query link: https://dune.com/queries/2634572


/*
queried on:
Balancer Pools https://dune.com/queries/2632759
*/
WITH arb_pools AS (
    SELECT 
        DISTINCT poolID AS "poolID",
        BYTEARRAY_SUBSTRING(poolID,1,20) AS pool_address,
        name,
        evt_block_time AS "pool_registered",
        'arbitrum' AS blockchain,
        SUM(pool_liquidity_usd) AS TVL
    FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered v
    LEFT JOIN balancer_v2_arbitrum.liquidity l
    ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id, 1, 20)
    LEFT JOIN labels.addresses a
    ON v.poolAddress = a.address AND l.blockchain = a.blockchain
    WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') AND l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
    GROUP BY 1, 2, 3, 4
),

eth_pools AS (
    SELECT 
        DISTINCT poolID AS "poolID",
        BYTEARRAY_SUBSTRING(poolID,1,20) AS pool_address,
        name,
        evt_block_time AS "pool_registered",
        'ethereum' AS blockchain,
        SUM(pool_liquidity_usd) AS TVL
    FROM balancer_v2_ethereum.Vault_evt_PoolRegistered v
    LEFT JOIN balancer_v2_ethereum.liquidity l
    ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id, 1, 20)
    LEFT JOIN labels.addresses a
    ON v.poolAddress = a.address AND l.blockchain = a.blockchain
    WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') AND l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
    GROUP BY 1, 2, 3, 4
    
    UNION ALL 
    SELECT 
    DISTINCT pool_id AS "poolID",
        BYTEARRAY_SUBSTRING(pool_id,1,20) AS pool_address,
        pool_symbol,
        CAST('2021-09-19' AS TIMESTAMP) AS "pool_registered",
        'ethereum' AS blockchain,
        SUM(pool_liquidity_usd) AS TVL
    FROM balancer_v1_ethereum.liquidity
    WHERE day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
    GROUP BY 1, 2, 3, 4
),

gno_pools AS (
    SELECT 
        DISTINCT poolID AS "poolID",
        BYTEARRAY_SUBSTRING(poolID,1,20) AS pool_address,
        name,
        evt_block_time AS "pool_registered",
        'gnosis' AS blockchain,
        SUM(pool_liquidity_usd) AS TVL
    FROM balancer_v2_gnosis.Vault_evt_PoolRegistered v
    LEFT JOIN balancer_v2_gnosis.liquidity l
    ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id, 1, 20)
    LEFT JOIN labels.addresses a
    ON v.poolAddress = a.address AND l.blockchain = a.blockchain
    WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') AND l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
    GROUP BY 1, 2, 3, 4
),

opt_pools AS (
    SELECT 
        DISTINCT poolID AS "poolID",
        BYTEARRAY_SUBSTRING(poolID,1,20) AS pool_address,
        name,
        evt_block_time AS "pool_registered",
        'optimism' AS blockchain,
        SUM(pool_liquidity_usd) AS TVL
    FROM balancer_v2_optimism.Vault_evt_PoolRegistered v
    LEFT JOIN balancer_v2_optimism.liquidity l
    ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id, 1, 20)
    LEFT JOIN labels.addresses a
    ON v.poolAddress = a.address AND l.blockchain = a.blockchain
    WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') AND l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
    GROUP BY 1, 2, 3, 4
),

pol_pools AS (
    SELECT 
        DISTINCT poolID AS "poolID",
        BYTEARRAY_SUBSTRING(poolID,1,20) AS pool_address,
        name,
        evt_block_time AS "pool_registered",
        'polygon' AS blockchain,
        SUM(pool_liquidity_usd) AS TVL
    FROM balancer_v2_polygon.Vault_evt_PoolRegistered v
    LEFT JOIN balancer_v2_polygon.liquidity l
    ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id, 1, 20)
    LEFT JOIN labels.addresses a
    ON v.poolAddress = a.address AND l.blockchain = a.blockchain
    WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') AND l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
    GROUP BY 1, 2, 3, 4
),

zkevm_pools AS (
    SELECT 
        DISTINCT poolID AS "poolID",
        BYTEARRAY_SUBSTRING(poolID,1,20) AS pool_address,
        name,
        evt_block_time AS "pool_registered",
        'zkevm' AS blockchain,
        SUM(pool_liquidity_usd) AS TVL
    FROM balancer_v2_zkevm.Vault_evt_PoolRegistered v
    LEFT JOIN balancer.liquidity l
    ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id, 1, 20)
    AND l.blockchain = 'zkevm'
    LEFT JOIN labels.addresses a
    ON v.poolAddress = a.address AND l.blockchain = a.blockchain
    WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') AND l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
    GROUP BY 1, 2, 3, 4
),

ava_pools AS (
    SELECT 
        DISTINCT poolID AS "poolID",
        BYTEARRAY_SUBSTRING(poolID,1,20) AS pool_address,
        name,
        evt_block_time AS "pool_registered",
        'avalanche_c' AS blockchain,
        SUM(pool_liquidity_usd) AS TVL
    FROM balancer_v2_avalanche_c.Vault_evt_PoolRegistered v
    LEFT JOIN balancer_v2_avalanche_c.liquidity l
    ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id, 1, 20)
    LEFT JOIN labels.addresses a
    ON v.poolAddress = a.address AND l.blockchain = a.blockchain
    WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') AND l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
    GROUP BY 1, 2, 3, 4
),

bas_pools AS (
    SELECT 
        DISTINCT poolID AS "poolID",
        BYTEARRAY_SUBSTRING(poolID,1,20) AS pool_address,
        name,
        evt_block_time AS "pool_registered",
        'base' AS blockchain,
        SUM(pool_liquidity_usd) AS TVL
    FROM balancer_v2_base.Vault_evt_PoolRegistered v
    LEFT JOIN balancer_v2_base.liquidity l
    ON v.poolAddress = BYTEARRAY_SUBSTRING(l.pool_id, 1, 20)
    LEFT JOIN labels.addresses a
    ON v.poolAddress = a.address AND l.blockchain = a.blockchain
    WHERE "category" IN ('balancer_v1_pool', 'balancer_v2_pool') AND l.day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
    GROUP BY 1, 2, 3, 4
)

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
SELECT * FROM ava_pools
UNION ALL
SELECT * FROM bas_pools
UNION ALL
SELECT * FROM zkevm_pools

ORDER BY 4 DESC;
