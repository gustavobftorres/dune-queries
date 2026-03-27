-- part of a query repo
-- query name: Total LBPs Volume
-- query link: https://dune.com/queries/186509


WITH lbps AS (
        -- V1 LBPs
        SELECT pool 
        FROM balancer.view_lbps
        
        UNION ALL
        
        -- V2 LBPs
        SELECT 
            "poolId" AS pool
        FROM balancer_v2."Vault_evt_PoolRegistered" c
        INNER JOIN balancer_v2."LiquidityBootstrappingPoolFactory_call_create" cc
        ON c.evt_tx_hash = cc.call_tx_hash
        AND cc.call_success
    )
    
SELECT SUM(usd_amount) AS volume
FROM dex.trades t
INNER JOIN lbps l 
ON l.pool = t.exchange_contract_address
AND project = 'Balancer'