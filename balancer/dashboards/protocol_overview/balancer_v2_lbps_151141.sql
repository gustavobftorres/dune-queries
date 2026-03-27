-- part of a query repo
-- query name: Balancer V2 LBPs
-- query link: https://dune.com/queries/151141


WITH lbps AS (
    SELECT 
        lower(symbol) AS name,
        "poolId" AS pool_id,
        SUBSTRING("poolId" FOR 20) AS pool_address
    FROM balancer_v2."Vault_evt_PoolRegistered" c
    INNER JOIN balancer_v2."LiquidityBootstrappingPoolFactory_call_create" cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        lower(symbol) AS name,
        "poolId" AS pool_id,
        SUBSTRING("poolId" FOR 20) AS pool_address
    FROM balancer_v2."Vault_evt_PoolRegistered" c
    INNER JOIN balancer_v2."NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create" cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND cc.call_success
)

SELECT name, 
CONCAT('<a href="https://duneanalytics.com/balancerlabs/balancer-v2-lbps?LBP=', name, '">view stats</a>') AS stats,
CONCAT('<a target="_blank" href="https://app.balancer.fi/#/pool/0', SUBSTRING(pool_id::text, 2), '">view pool</a>') AS pool,
CONCAT('<a target="_blank" href="https://etherscan.io/address/0', SUBSTRING(pool_address::text, 2, 42), '">0', SUBSTRING(pool_address::text, 2, 42), '</a>') AS etherscan
FROM lbps