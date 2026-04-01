-- part of a query repo
-- query name: 8020 Pools
-- query link: https://dune.com/queries/3090095


WITH weighted_pools as(
SELECT output_0, name, 'arbitrum' as blockchain
FROM balancer_v2_arbitrum.WeightedPoolFactory_call_create
WHERE ((CAST(normalizedWeights[1] as bigint) = 800000000000000000
AND CAST(normalizedWeights[2] as bigint) = 200000000000000000
OR CAST(normalizedWeights[2] as bigint) = 800000000000000000
AND CAST(normalizedWeights[1] as bigint) = 200000000000000000)
OR CAST(weights[1] as bigint) = 800000000000000000
AND CAST(weights[2] as bigint) = 200000000000000000
OR CAST(weights[2] as bigint) = 800000000000000000
AND CAST(weights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'avalanche_c' as blockchain
FROM balancer_v2_avalanche_c.WeightedPoolFactory_call_create
WHERE (CAST(normalizedWeights[1] as bigint) = 800000000000000000
AND CAST(normalizedWeights[2] as bigint) = 200000000000000000
OR CAST(normalizedWeights[2] as bigint) = 800000000000000000
AND CAST(normalizedWeights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'ethereum' as blockchain
FROM balancer_v2_ethereum.WeightedPoolFactory_call_create
WHERE ((CAST(normalizedWeights[1] as bigint) = 800000000000000000
AND CAST(normalizedWeights[2] as bigint) = 200000000000000000
OR CAST(normalizedWeights[2] as bigint) = 800000000000000000
AND CAST(normalizedWeights[1] as bigint) = 200000000000000000)
OR CAST(weights[1] as bigint) = 800000000000000000
AND CAST(weights[2] as bigint) = 200000000000000000
OR CAST(weights[2] as bigint) = 800000000000000000
AND CAST(weights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'base' as blockchain
FROM balancer_v2_base.WeightedPoolFactory_call_create
WHERE (CAST(normalizedWeights[1] as bigint) = 800000000000000000
AND CAST(normalizedWeights[2] as bigint) = 200000000000000000
OR CAST(normalizedWeights[2] as bigint) = 800000000000000000
AND CAST(normalizedWeights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'optimism' as blockchain
FROM balancer_v2_optimism.WeightedPoolFactory_call_create
WHERE ((CAST(normalizedWeights[1] as bigint) = 800000000000000000
AND CAST(normalizedWeights[2] as bigint) = 200000000000000000
OR CAST(normalizedWeights[2] as bigint) = 800000000000000000
AND CAST(normalizedWeights[1] as bigint) = 200000000000000000)
OR CAST(weights[1] as bigint) = 800000000000000000
AND CAST(weights[2] as bigint) = 200000000000000000
OR CAST(weights[2] as bigint) = 800000000000000000
AND CAST(weights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'polygon' as blockchain
FROM balancer_v2_polygon.WeightedPoolFactory_call_create
WHERE ((CAST(normalizedWeights[1] as bigint) = 800000000000000000
AND CAST(normalizedWeights[2] as bigint) = 200000000000000000
OR CAST(normalizedWeights[2] as bigint) = 800000000000000000
AND CAST(normalizedWeights[1] as bigint) = 200000000000000000)
OR CAST(weights[1] as bigint) = 800000000000000000
AND CAST(weights[2] as bigint) = 200000000000000000
OR CAST(weights[2] as bigint) = 800000000000000000
AND CAST(weights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'arbitrum' as blockchain
FROM balancer_v2_arbitrum.WeightedPool2TokensFactory_call_create
WHERE (CAST(weights[1] as bigint) = 800000000000000000
AND CAST(weights[2] as bigint) = 200000000000000000
OR CAST(weights[2] as bigint) = 800000000000000000
AND CAST(weights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'arbitrum' as blockchain
FROM balancer_v2_arbitrum.WeightedPoolV2Factory_call_create
WHERE (CAST(normalizedWeights[1] as bigint) = 800000000000000000
AND CAST(normalizedWeights[2] as bigint) = 200000000000000000
OR CAST(normalizedWeights[2] as bigint) = 800000000000000000
AND CAST(normalizedWeights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'ethereum' as blockchain
FROM balancer_v2_ethereum.WeightedPool2TokensFactory_call_create
WHERE (CAST(weights[1] as bigint) = 800000000000000000
AND CAST(weights[2] as bigint) = 200000000000000000
OR CAST(weights[2] as bigint) = 800000000000000000
AND CAST(weights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'gnosis' as blockchain
FROM balancer_v2_gnosis.WeightedPoolV2Factory_call_create
WHERE (CAST(normalizedWeights[1] as bigint) = 800000000000000000
AND CAST(normalizedWeights[2] as bigint) = 200000000000000000
OR CAST(normalizedWeights[2] as bigint) = 800000000000000000
AND CAST(normalizedWeights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'gnosis' as blockchain
FROM balancer_v2_gnosis.WeightedPoolV4Factory_call_create
WHERE (CAST(normalizedWeights[1] as bigint) = 800000000000000000
AND CAST(normalizedWeights[2] as bigint) = 200000000000000000
OR CAST(normalizedWeights[2] as bigint) = 800000000000000000
AND CAST(normalizedWeights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'optimism' as blockchain
FROM balancer_v2_optimism.WeightedPool2TokensFactory_call_create
WHERE (CAST(weights[1] as bigint) = 800000000000000000
AND CAST(weights[2] as bigint) = 200000000000000000
OR CAST(weights[2] as bigint) = 800000000000000000
AND CAST(weights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'optimism' as blockchain
FROM balancer_v2_optimism.WeightedPoolV2Factory_call_create
WHERE (CAST(normalizedWeights[1] as bigint) = 800000000000000000
AND CAST(normalizedWeights[2] as bigint) = 200000000000000000
OR CAST(normalizedWeights[2] as bigint) = 800000000000000000
AND CAST(normalizedWeights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'polygon' as blockchain
FROM balancer_v2_polygon.WeightedPoolV2Factory_call_create
WHERE (CAST(normalizedWeights[1] as bigint) = 800000000000000000
AND CAST(normalizedWeights[2] as bigint) = 200000000000000000
OR CAST(normalizedWeights[2] as bigint) = 800000000000000000
AND CAST(normalizedWeights[1] as bigint) = 200000000000000000)
UNION ALL  
SELECT output_0, name, 'polygon' as blockchain
FROM balancer_v2_polygon.WeightedPool2TokensFactory_call_create
WHERE (CAST(weights[1] as bigint) = 800000000000000000
AND CAST(weights[2] as bigint) = 200000000000000000
OR CAST(weights[2] as bigint) = 800000000000000000
AND CAST(weights[1] as bigint) = 200000000000000000)
),

tvl as(
SELECT l.pool_id, BYTEARRAY_SUBSTRING(l.pool_id,1,20) as pool_address, l.blockchain, SUM(l.pool_liquidity_usd) as tvl
FROM balancer.liquidity l
WHERE l.pool_liquidity_usd > 1 
AND l.day = (CURRENT_DATE - interval '1' day)
GROUP BY 1,2,3
),

swaps as(
SELECT t.project_contract_address, t.blockchain,
SUM(CASE WHEN t.block_time >= (now() - INTERVAL '24' hour) THEN t.amount_usd ELSE 0 END) as "24h_volume",
SUM(CASE WHEN t.block_time >= (now() - INTERVAL '30' day) THEN t.amount_usd ELSE 0 END) as "30d_volume"
FROM balancer.trades t
GROUP BY 1,2
)

SELECT w.*, SUM(t.tvl) as tvl, SUM(s."24h_volume") as "24h_volume" , SUM(s."30d_volume") as "30d_volume", 
SUM(s."24h_volume")/SUM(t.tvl) as liq_util, CONCAT('<a target="_blank" href="https://dune.com/balancer/pool-analysis?1.+Pool+ID_ta1851=', SUBSTRING(CAST(t.pool_id as VARCHAR), 1,66),
        '&4.+Blockchain_t68adc=', w.blockchain, '">View Stats ↗</a>') AS stats
FROM weighted_pools w
LEFT JOIN tvl t ON t.pool_address = w.output_0 AND t.blockchain = w.blockchain 
LEFT JOIN swaps s ON s.project_contract_address = w.output_0 AND s.blockchain = w.blockchain
WHERE t.tvl > 1000 AND s."30d_volume" > 1000
GROUP BY 1,2,3,8
ORDER BY 4 DESC

