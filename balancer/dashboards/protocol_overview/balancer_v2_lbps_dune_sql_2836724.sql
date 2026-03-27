-- part of a query repo
-- query name: Balancer V2 LBPs (Dune SQL)
-- query link: https://dune.com/queries/2836724


-- query_2452021
WITH lbps AS (
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as varchar) AS pool_id,
        SUBSTRING(CAST("poolId" as VARCHAR), 1,42) AS pool_address,
        'ethereum' as blockchain
    FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_ethereum.LiquidityBootstrappingPoolFactory_call_create cc 
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        SUBSTRING(CAST("poolId" as VARCHAR), 1,42) AS pool_address,
        'ethereum' as blockchain
    FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_ethereum.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as varchar) AS pool_id,
        SUBSTRING(CAST("poolId" as VARCHAR), 1,42) AS pool_address,
        'arbitrum' as blockchain
    FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_arbitrum.LiquidityBootstrappingPoolFactory_call_create cc 
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        SUBSTRING(CAST("poolId" as VARCHAR), 1,42) AS pool_address,
        'arbitrum' as blockchain
    FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_arbitrum.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        SUBSTRING(CAST("poolId" as VARCHAR), 1,42) AS pool_address,
        'avalanche_c' as blockchain
    FROM balancer_v2_avalanche_c.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_avalanche_c.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        SUBSTRING(CAST("poolId" as VARCHAR), 1,42) AS pool_address,
        'base' as blockchain
    FROM balancer_v2_base.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_base.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        SUBSTRING(CAST("poolId" as VARCHAR), 1,42) AS pool_address,
        'gnosis' as blockchain
    FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_gnosis.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        SUBSTRING(CAST("poolId" as VARCHAR), 1,42) AS pool_address,
        'optimism' as blockchain
    FROM balancer_v2_optimism.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_optimism.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as varchar) AS pool_id,
        SUBSTRING(CAST("poolId" as VARCHAR), 1,42) AS pool_address,
        'polygon' as blockchain
    FROM balancer_v2_polygon.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_polygon.LiquidityBootstrappingPoolFactory_call_create cc 
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        SUBSTRING(CAST("poolId" as VARCHAR), 1,42) AS pool_address,
        'polygon' as blockchain
    FROM balancer_v2_polygon.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_polygon.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success

)

SELECT name, 
blockchain,
CONCAT('<a href="https://dune.com/balancer/balancer-v2-lbps?LBP_t8843e=', name, '">view stats</a>') AS stats,
CASE WHEN blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/ethereum/pool/0', SUBSTRING(CAST (pool_id as VARCHAR), 2), '">balancer ↗</a>') 
WHEN blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/arbitrum/pool/0', SUBSTRING(CAST (pool_id as VARCHAR), 2), '">balancer ↗</a>')
WHEN blockchain = 'polygon' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/polygon/pool/0', SUBSTRING(CAST (pool_id as VARCHAR), 2), '">balancer ↗</a>')
WHEN blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/gnosis/pool/0', SUBSTRING(CAST (pool_id as VARCHAR), 2), '">balancer ↗</a>')
END AS pool,
    CASE WHEN blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://etherscan.io/address/0', SUBSTRING(CAST(pool_id as VARCHAR), 2, 41), '">etherscan ↗</a>')
    WHEN blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://arbiscan.io/address/0', SUBSTRING(CAST(pool_id as VARCHAR), 2, 41), '">arbiscan ↗</a>')
    WHEN blockchain = 'polygon' THEN CONCAT('<a target="_blank" href="https://polygonscan.com/address/0', SUBSTRING(CAST(pool_id as VARCHAR), 2, 41), '">polygonscan ↗</a>')
    WHEN blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://gnosisscan.com/address/0', SUBSTRING(CAST(pool_id as VARCHAR), 2, 41), '">gnosisscan ↗</a>')
     WHEN blockchain = 'optimism' THEN CONCAT('<a target="_blank" href="https://optimistic.etherscan.io/address/0', SUBSTRING(CAST(pool_id as VARCHAR), 2, 41), '">optimistic ↗</a>')
    END AS Scan,
pool_id
FROM lbps
ORDER BY 1 DESC