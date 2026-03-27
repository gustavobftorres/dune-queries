-- part of a query repo
-- query name: get rate providers for stable pools
-- query link: https://dune.com/queries/3460011


WITH pools as(
SELECT 'ethereum' AS blockchain, c.output_0 AS pool_address, name, token_address, call_tx_hash, rate_provider
FROM balancer_v2_ethereum.ComposableStablePoolFactory_call_create c
CROSS JOIN UNNEST(tokens) WITH ORDINALITY t(token_address,pos)
CROSS JOIN UNNEST(rateProviders) WITH ORDINALITY p(rate_provider, pos)
WHERE t.pos = p.pos

UNION ALL

SELECT 'ethereum' AS blockchain, c.output_0 AS pool_address, name, token_address, call_tx_hash, rate_provider
FROM balancer_v2_ethereum.StablePhantomPoolFactory_call_create c
CROSS JOIN UNNEST(tokens) WITH ORDINALITY t(token_address,pos)
CROSS JOIN UNNEST(rateProviders) WITH ORDINALITY p(rate_provider, pos)
WHERE t.pos = p.pos

UNION ALL

SELECT 'polygon' AS blockchain, c.output_0 AS pool_address, name, token_address, call_tx_hash, rate_provider
FROM balancer_v2_polygon.StablePhantomPoolFactory_call_create c
CROSS JOIN UNNEST(tokens) WITH ORDINALITY t(token_address,pos)
CROSS JOIN UNNEST(rateProviders) WITH ORDINALITY p(rate_provider, pos)
WHERE t.pos = p.pos

UNION ALL

SELECT 'arbitrum' AS blockchain, c.output_0 AS pool_address, name, token_address, call_tx_hash, rate_provider
FROM balancer_v2_arbitrum.ComposableStablePoolFactory_call_create c
CROSS JOIN UNNEST(tokens) WITH ORDINALITY t(token_address,pos)
CROSS JOIN UNNEST(rateProviders) WITH ORDINALITY p(rate_provider, pos)
WHERE t.pos = p.pos

UNION ALL

SELECT 'optimism', c.output_0 AS pool_address, name, token_address, call_tx_hash, rate_provider
FROM balancer_v2_optimism.ComposableStablePoolFactory_call_create c
CROSS JOIN UNNEST(tokens) WITH ORDINALITY t(token_address,pos)
CROSS JOIN UNNEST(rateProviders) WITH ORDINALITY p(rate_provider, pos)
WHERE t.pos = p.pos

UNION ALL

SELECT 'polygon' AS blockchain, c.output_0 AS pool_address, name, token_address, call_tx_hash, rate_provider
FROM balancer_v2_polygon.ComposableStablePoolFactory_call_create c
CROSS JOIN UNNEST(tokens) WITH ORDINALITY t(token_address,pos)
CROSS JOIN UNNEST(rateProviders) WITH ORDINALITY p(rate_provider, pos)
WHERE t.pos = p.pos

UNION ALL

SELECT 'gnosis' AS blockchain, c.output_0 AS pool_address, name, token_address, call_tx_hash, rate_provider
FROM balancer_v2_gnosis.ComposableStablePoolFactory_call_create c
CROSS JOIN UNNEST(tokens) WITH ORDINALITY t(token_address,pos)
CROSS JOIN UNNEST(rateProviders) WITH ORDINALITY p(rate_provider, pos)
WHERE t.pos = p.pos

UNION ALL

SELECT 'base' AS blockchain, c.output_0 AS pool_address, name, token_address, call_tx_hash, rate_provider
FROM balancer_v2_base.ComposableStablePoolFactory_call_create c
CROSS JOIN UNNEST(tokens) WITH ORDINALITY t(token_address,pos)
CROSS JOIN UNNEST(rateProviders) WITH ORDINALITY p(rate_provider, pos)
WHERE t.pos = p.pos

UNION ALL

SELECT 'avalanche_c' AS blockchain, c.output_0 AS pool_address, name, token_address, call_tx_hash, rate_provider
FROM balancer_v2_avalanche_c.ComposableStablePoolFactory_call_create c
CROSS JOIN UNNEST(tokens) WITH ORDINALITY t(token_address,pos)
CROSS JOIN UNNEST(rateProviders) WITH ORDINALITY p(rate_provider, pos)
WHERE t.pos = p.pos

UNION ALL

SELECT 'gnosis' AS blockchain, c.output_0 AS pool_address, name, token_address, call_tx_hash, rate_provider
FROM balancer_v2_gnosis.ComposableStablePoolV2Factory_call_create c
CROSS JOIN UNNEST(tokens) WITH ORDINALITY t(token_address,pos)
CROSS JOIN UNNEST(rateProviders) WITH ORDINALITY p(rate_provider, pos)
WHERE t.pos = p.pos
)

SELECT 
    p.blockchain,
    p.pool_address,
    p.name,
    p.token_address,
    t.symbol,
    p.rate_provider,
    CASE
            WHEN p.blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://etherscan.io/address/', CAST(rate_provider AS VARCHAR), '#readContract#F1', '">⛓</a>')
            WHEN p.blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://arbiscan.io/address/', CAST(rate_provider AS VARCHAR), '#readContract#F1', '">⛓</a>')
            WHEN p.blockchain = 'polygon' THEN CONCAT('<a target "_blank" href="https://polygonscan.com/address/', CAST(rate_provider AS VARCHAR), '#readContract#F1', '">⛓</a>')
            WHEN p.blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://gnosisscan.com/address/', CAST(rate_provider AS VARCHAR), '#readContract#F1', '">⛓</a>')
            WHEN p.blockchain = 'optimism' THEN CONCAT('<a target="_blank" href="https://optimistic.etherscan.io/address/', CAST(rate_provider AS VARCHAR), '#readContract#F1', '">⛓</a>')
            WHEN p.blockchain = 'avalanche_c' THEN CONCAT('<a target="_blank" href="https://snowtrace.io/address/', CAST(rate_provider AS VARCHAR), '#readContract#F1', '">⛓</a>')
            WHEN p.blockchain = 'base' THEN CONCAT('<a target="_blank" href="https://basescan.org/address/', CAST(rate_provider AS VARCHAR), '#readContract#F1', '">⛓</a>')
        END AS scan
FROM pools p
LEFT JOIN tokens.erc20 t ON t.blockchain = p.blockchain AND p.token_address = t.contract_address
WHERE rate_provider != 0x0000000000000000000000000000000000000000
AND ('{{Blockchain}}' = 'All' OR p.blockchain = '{{Blockchain}}')
ORDER BY 1 DESC, 2 DESC NULLS LAST