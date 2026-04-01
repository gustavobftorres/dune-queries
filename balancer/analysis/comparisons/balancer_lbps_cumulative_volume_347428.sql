-- part of a query repo
-- query name: Balancer LBPs Cumulative Volume
-- query link: https://dune.com/queries/347428


WITH lbps AS (
        -- V1 LBPs
        SELECT pool, name, 'V1' AS bal_version
        FROM balancer.view_lbps
        
        UNION ALL
        
        -- V2 LBPs
        SELECT 
            "poolId" AS pool,
            symbol AS name,
            'V2' AS bal_version
        FROM balancer_v2."Vault_evt_PoolRegistered" c
        INNER JOIN balancer_v2."LiquidityBootstrappingPoolFactory_call_create" cc
        ON c.evt_tx_hash = cc.call_tx_hash
        AND cc.call_success
        
        UNION ALL
        
        SELECT 
            "poolId" AS pool,
            symbol AS name,
            'V2' AS bal_version
        FROM balancer_v2."Vault_evt_PoolRegistered" c
        INNER JOIN balancer_v2."NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create" cc
        ON c.evt_tx_hash = cc.call_tx_hash
        AND cc.call_success
    ),
    
    volume AS (
        SELECT 
            date_trunc('week', block_time) AS day,
            SUM(usd_amount) AS volume
        FROM dex.trades t
        INNER JOIN lbps l 
        ON l.pool = t.exchange_contract_address
        AND project = 'Balancer'
        GROUP BY 1
    )
    
SELECT 
    day, 
    SUM(volume) OVER (ORDER BY day) AS volume
FROM volume
