-- part of a query repo
-- query name: Balancer V2 LBPs
-- query link: https://dune.com/queries/2452021


-- query_2452021
WITH lbps AS (
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as varchar) AS pool_id,
        SUBSTRING(CAST("poolId" as VARCHAR), 1,42) AS pool_address
    FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_ethereum.LiquidityBootstrappingPoolFactory_call_create cc 
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        SUBSTRING(CAST("poolId" as VARCHAR), 1,42) AS pool_address
    FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_ethereum.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
)

SELECT name, 
CONCAT('<a href="https://duneanalytics.com/balancerlabs/balancer-v2-lbps?LBP=', name, '">view stats</a>') AS stats,
CONCAT('<a target="_blank" href="https://app.balancer.fi/#/pool/0', SUBSTRING(pool_id, 2), '">view pool</a>') AS pool,
CONCAT('<a target="_blank" href="https://etherscan.io/address/', SUBSTRING(pool_id, 1, 42), '">view on etherscan</a>') AS etherscan,
pool_id
FROM lbps