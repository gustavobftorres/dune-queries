-- part of a query repo
-- query name: (query_ 3954616) b_cow_amm_pool_data
-- query link: https://dune.com/queries/3954616


/*
queried on:
Balancer Pools https://dune.com/queries/2632759
*/
WITH arb_pools AS (
    SELECT 
        DISTINCT bPool AS "poolID",
        BYTEARRAY_SUBSTRING(bPool,1,20) AS pool_address,
        name,
        evt_block_time AS "pool_registered",
        'arbitrum' AS blockchain,
        SUM(protocol_liquidity_usd) AS TVL
    FROM b_cow_amm_arbitrum.BCoWFactory_evt_LOG_NEW_POOL v
    LEFT JOIN balancer_cowswap_amm_arbitrum.liquidity l
    ON v.bPool = BYTEARRAY_SUBSTRING(l.pool_id, 1, 20)
    LEFT JOIN labels.balancer_cowswap_amm_pools_arbitrum a
    ON v.bPool = a.address AND l.blockchain = a.blockchain
    WHERE l.day = (SELECT MAX(day) FROM balancer_cowswap_amm_arbitrum.liquidity)
    GROUP BY 1, 2, 3, 4
),

eth_pools AS (
    SELECT 
        DISTINCT bPool AS "poolID",
        BYTEARRAY_SUBSTRING(bPool,1,20) AS pool_address,
        name,
        evt_block_time AS "pool_registered",
        'ethereum' AS blockchain,
        SUM(protocol_liquidity_usd) AS TVL
    FROM b_cow_amm_ethereum.BCoWFactory_evt_LOG_NEW_POOL v
    LEFT JOIN balancer_cowswap_amm_ethereum.liquidity l
    ON v.bPool = BYTEARRAY_SUBSTRING(l.pool_id, 1, 20)
    LEFT JOIN labels.balancer_cowswap_amm_pools_ethereum a
    ON v.bPool = a.address AND l.blockchain = a.blockchain
    WHERE l.day = (SELECT MAX(day) FROM balancer_cowswap_amm_ethereum.liquidity)
    GROUP BY 1, 2, 3, 4
),

gno_pools AS (
    SELECT 
        DISTINCT bPool AS "poolID",
        BYTEARRAY_SUBSTRING(bPool,1,20) AS pool_address,
        name,
        evt_block_time AS "pool_registered",
        'gnosis' AS blockchain,
        SUM(protocol_liquidity_usd) AS TVL
    FROM b_cow_amm_gnosis.BCoWFactory_evt_LOG_NEW_POOL v
    LEFT JOIN balancer_cowswap_amm_gnosis.liquidity l
    ON v.bPool = BYTEARRAY_SUBSTRING(l.pool_id, 1, 20)
    LEFT JOIN labels.balancer_cowswap_amm_pools_gnosis a
    ON v.bPool = a.address AND l.blockchain = a.blockchain
    WHERE l.day = (SELECT MAX(day) FROM balancer_cowswap_amm_gnosis.liquidity)
    GROUP BY 1, 2, 3, 4
),

base_pools AS (
    SELECT 
        DISTINCT bPool AS "poolID",
        BYTEARRAY_SUBSTRING(bPool,1,20) AS pool_address,
        name,
        evt_block_time AS "pool_registered",
        'base' AS blockchain,
        SUM(protocol_liquidity_usd) AS TVL
    FROM b_cow_amm_base.BCoWFactory_evt_LOG_NEW_POOL v
    LEFT JOIN balancer_cowswap_amm_base.liquidity l
    ON v.bPool = BYTEARRAY_SUBSTRING(l.pool_id, 1, 20)
    LEFT JOIN labels.balancer_cowswap_amm_pools_base a
    ON v.bPool = a.address AND l.blockchain = a.blockchain
    WHERE l.day = (SELECT MAX(day) FROM balancer_cowswap_amm_base.liquidity)
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM arb_pools
UNION ALL
SELECT * FROM eth_pools
UNION ALL
SELECT * FROM gno_pools
UNION ALL
SELECT * FROM base_pools

ORDER BY 4 DESC;
