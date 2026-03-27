-- part of a query repo
-- query name: LST Pools
-- query link: https://dune.com/queries/3372434


WITH lst_pools AS(
SELECT DISTINCT output_0 AS pool_address, token_address, blockchain, name FROM (
SELECT output_0, name, token_address, 'ethereum' AS blockchain FROM balancer_v2_ethereum.ComposableStablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'ethereum' AS blockchain FROM balancer_v2_ethereum.StablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'ethereum' AS blockchain FROM balancer_v2_ethereum.MetaStablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'ethereum' AS blockchain FROM balancer_v2_ethereum.StablePhantomPoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'ethereum' AS blockchain FROM gyroscope_ethereum.GyroECLPPoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'arbitrum' AS blockchain FROM gyroscope_arbitrum.GyroECLPPoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'arbitrum' AS blockchain FROM balancer_v2_arbitrum.ComposableStablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'arbitrum' AS blockchain FROM balancer_v2_arbitrum.StablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'arbitrum' AS blockchain FROM balancer_v2_arbitrum.MetaStablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'optimism' AS blockchain FROM gyroscope_optimism.GyroECLPPoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'optimism' AS blockchain FROM balancer_v2_optimism.ComposableStablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'optimism' AS blockchain FROM balancer_v2_optimism.StablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'optimism' AS blockchain FROM balancer_v2_optimism.MetaStablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'avalanche_c' AS blockchain FROM balancer_v2_avalanche_c.ComposableStablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'base' AS blockchain FROM balancer_v2_base.ComposableStablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'polygon' AS blockchain FROM gyroscope_polygon.GyroECLPPoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'polygon' AS blockchain FROM balancer_v2_polygon.ComposableStablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'polygon' AS blockchain FROM balancer_v2_polygon.StablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'polygon' AS blockchain FROM balancer_v2_polygon.MetaStablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'polygon' AS blockchain FROM balancer_v2_polygon.StablePhantomPoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'gnosis' AS blockchain FROM balancer_v2_gnosis.ComposableStablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'gnosis' AS blockchain FROM balancer_v2_gnosis.ComposableStablePoolV2Factory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'gnosis' AS blockchain FROM balancer_v2_gnosis.StablePoolV2Factory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)

UNION ALL

SELECT output_0, name, token_address, 'zkevm' AS blockchain FROM balancer_v2_zkevm.ComposableStablePoolFactory_call_create
CROSS JOIN UNNEST(tokens) AS t(token_address)
)

WHERE name NOT LIKE 'DO NOT USE%' AND output_0 IS NOT NULL
    AND name NOT LIKE '%Test%')

SELECT 
    DISTINCT pool_address AS pool_address,
    l.blockchain,
    name
FROM lst_pools l
INNER JOIN dune.balancer.result_lst_tokens t ON l.token_address = t.contract_address
AND l.blockchain = t.blockchain

