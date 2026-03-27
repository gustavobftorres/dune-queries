-- part of a query repo
-- query name: Missing tokens on tokens.erc20
-- query link: https://dune.com/queries/3500282


WITH registered_tokens AS(
    SELECT 
        poolId, 
        'arbitrum' AS blockchain, 
        CAST(t.token AS VARCHAR) AS token, '2' AS version
    FROM balancer_v2_arbitrum.Vault_evt_TokensRegistered
    CROSS JOIN UNNEST(tokens) AS t(token)
    WHERE evt_block_time > now() - interval '14' day
    
    UNION ALL
    
    SELECT 
        poolId, 
        'avalanche_c' AS blockchain, 
        CAST(t.token AS VARCHAR) AS token, '2' AS version
    FROM balancer_v2_avalanche_c.Vault_evt_TokensRegistered
    CROSS JOIN UNNEST(tokens) AS t(token)
    WHERE evt_block_time > now() - interval '14' day
    
    UNION ALL
    
    SELECT 
        poolId, 
        'base' AS blockchain, 
        CAST(t.token AS VARCHAR) AS token, '2' AS version
    FROM balancer_v2_base.Vault_evt_TokensRegistered
    CROSS JOIN UNNEST(tokens) AS t(token)
    WHERE evt_block_time > now() - interval '14' day
    
    UNION ALL
    
        SELECT 
        poolId, 
        'ethereum' AS blockchain, 
        CAST(t.token AS VARCHAR) AS token, '2' AS version
    FROM balancer_v2_ethereum.Vault_evt_TokensRegistered
    CROSS JOIN UNNEST(tokens) AS t(token)
    WHERE evt_block_time > now() - interval '14' day
    
    UNION ALL
    
    SELECT 
        poolId, 
        'gnosis' AS blockchain, 
        CAST(t.token AS VARCHAR) AS token, '2' AS version
    FROM balancer_v2_gnosis.Vault_evt_TokensRegistered
    CROSS JOIN UNNEST(tokens) AS t(token)
    WHERE evt_block_time > now() - interval '14' day
    
    UNION ALL
    
    SELECT 
        poolId, 
        'optimism' AS blockchain, 
        CAST(t.token AS VARCHAR) AS token, '2' AS version
    FROM balancer_v2_optimism.Vault_evt_TokensRegistered
    CROSS JOIN UNNEST(tokens) AS t(token)
    WHERE evt_block_time > now() - interval '14' day
    
    UNION ALL
    
    SELECT 
        poolId, 
        'polygon' AS blockchain, 
        CAST(t.token AS VARCHAR) AS token, '2' AS version
    FROM balancer_v2_polygon.Vault_evt_TokensRegistered
    CROSS JOIN UNNEST(tokens) AS t(token)
    WHERE evt_block_time > now() - interval '14' day
    
    UNION ALL
    
    SELECT 
        poolId, 
        'zkevm' AS blockchain, 
        CAST(t.token AS VARCHAR) AS token, '2' AS version
    FROM balancer_v2_zkevm.Vault_evt_TokensRegistered
    CROSS JOIN UNNEST(tokens) AS t(token)
    WHERE evt_block_time > now() - interval '14' day

    UNION ALL
    
    SELECT
            pool,
            'ethereum' AS blockchain,
            json_extract_scalar(token, '$.token') AS token,
            '3' AS version
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM balancer_v3_ethereum.Vault_evt_PoolRegistered
            WHERE evt_block_time > now() - interval '14' day
            ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1, 2, 3

    UNION ALL

        SELECT
            pool,
            'gnosis' AS blockchain,
            json_extract_scalar(token, '$.token') AS token,
            '3' AS version
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM balancer_v3_gnosis.Vault_evt_PoolRegistered
            WHERE evt_block_time > now() - interval '14' day
            ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1, 2, 3

    UNION ALL

        SELECT
            pool,
            'arbitrum' AS blockchain,
            json_extract_scalar(token, '$.token') AS token,
            '3' AS version
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM balancer_v3_arbitrum.Vault_evt_PoolRegistered
            WHERE evt_block_time > now() - interval '14' day
            ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1, 2, 3        

        UNION ALL

        SELECT
            pool,
            'base' AS blockchain,
            json_extract_scalar(token, '$.token') AS token,
            '3' AS version
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM balancer_v3_base.Vault_evt_PoolRegistered
            WHERE evt_block_time > now() - interval '14' day
            ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1, 2, 3        
    )
    
SELECT
    DISTINCT r.token, r.blockchain, version
FROM registered_tokens r
LEFT JOIN tokens.erc20 t
ON LOWER(r.token) = LOWER(CAST(t.contract_address AS VARCHAR))
AND r.blockchain = t.blockchain
LEFT JOIN labels.balancer_v2_pools l 
ON LOWER(CAST(l.address AS VARCHAR)) = LOWER(r.token)
AND l.blockchain = r.blockchain
WHERE symbol IS NULL AND l.name IS NULL --to eliminate BPTs and tokens already on tokens.erc20
ORDER BY 3 DESC, 2 ASC
