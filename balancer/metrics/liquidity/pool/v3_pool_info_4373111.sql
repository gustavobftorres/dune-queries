-- part of a query repo
-- query name: V3 Pool Info
-- query link: https://dune.com/queries/4373111


WITH eth_pools AS (
 WITH token_data AS (
        SELECT
            pool,
            ARRAY_AGG(json_extract_scalar(token, '$.token') ORDER BY token_index) AS tokens,
            ARRAY_AGG(json_extract_scalar(token, '$.rateProvider') ORDER BY token_index) AS rate_providers
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM balancer_testnet_sepolia.Vault_evt_PoolRegistered
        ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1
    )

SELECT 
    'ethereum' AS blockchain,
    p.pool,
    factory,
    evt_block_time,
    evt_block_number,
    td.tokens,
    td.rate_providers,
    swapFeePercentage / 1e18 AS swapFeePercentage,
    ARRAY_AGG(json_extract_scalar(roleAccounts, '$.poolCreator')) AS pool_creator,
    ARRAY_AGG(json_extract_scalar(hooksConfig, '$.hooksContract')) AS hook_address
FROM balancer_v3_ethereum.Vault_evt_PoolRegistered p
JOIN token_data td ON p.pool = td.pool
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8),

gno_pools AS (
 WITH token_data AS (
        SELECT
            pool,
            ARRAY_AGG(json_extract_scalar(token, '$.token') ORDER BY token_index) AS tokens,
            ARRAY_AGG(json_extract_scalar(token, '$.rateProvider') ORDER BY token_index) AS rate_providers
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM balancer_v3_gnosis.Vault_evt_PoolRegistered
        ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1
    )

SELECT 
    'gnosis' AS blockchain,
    p.pool,
    factory,
    evt_block_time,
    evt_block_number,
    td.tokens,
    td.rate_providers,
    swapFeePercentage / 1e18 AS swapFeePercentage,
    ARRAY_AGG(json_extract_scalar(roleAccounts, '$.poolCreator')) AS pool_creator,
    ARRAY_AGG(json_extract_scalar(hooksConfig, '$.hooksContract')) AS hook_address
FROM balancer_v3_gnosis.Vault_evt_PoolRegistered p
JOIN token_data td ON p.pool = td.pool
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8),

arb_pools AS (
 WITH token_data AS (
        SELECT
            pool,
            ARRAY_AGG(json_extract_scalar(token, '$.token') ORDER BY token_index) AS tokens,
            ARRAY_AGG(json_extract_scalar(token, '$.rateProvider') ORDER BY token_index) AS rate_providers
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM balancer_v3_arbitrum.Vault_evt_PoolRegistered
        ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1
    )

SELECT 
    'arbitrum' AS blockchain,
    p.pool,
    factory,
    evt_block_time,
    evt_block_number,
    td.tokens,
    td.rate_providers,
    swapFeePercentage / 1e18 AS swapFeePercentage,
    ARRAY_AGG(json_extract_scalar(roleAccounts, '$.poolCreator')) AS pool_creator,
    ARRAY_AGG(json_extract_scalar(hooksConfig, '$.hooksContract')) AS hook_address
FROM balancer_v3_arbitrum.Vault_evt_PoolRegistered p
JOIN token_data td ON p.pool = td.pool
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8),

base_pools AS (
 WITH token_data AS (
        SELECT
            pool,
            ARRAY_AGG(json_extract_scalar(token, '$.token') ORDER BY token_index) AS tokens,
            ARRAY_AGG(json_extract_scalar(token, '$.rateProvider') ORDER BY token_index) AS rate_providers
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM balancer_v3_base.Vault_evt_PoolRegistered
        ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1
    )

SELECT 
    'base' AS blockchain,
    p.pool,
    factory,
    evt_block_time,
    evt_block_number,
    td.tokens,
    td.rate_providers,
    swapFeePercentage / 1e18 AS swapFeePercentage,
    ARRAY_AGG(json_extract_scalar(roleAccounts, '$.poolCreator')) AS pool_creator,
    ARRAY_AGG(json_extract_scalar(hooksConfig, '$.hooksContract')) AS hook_address
FROM balancer_v3_base.Vault_evt_PoolRegistered p
JOIN token_data td ON p.pool = td.pool
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8)

SELECT * FROM eth_pools
UNION
SELECT * FROM gno_pools
UNION
SELECT * FROM arb_pools
UNION
SELECT * FROM base_pools
ORDER BY 4 DESC