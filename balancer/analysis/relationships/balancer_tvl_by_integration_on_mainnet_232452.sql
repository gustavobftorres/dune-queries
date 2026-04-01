-- part of a query repo
-- query name: Balancer TVL by Integration on Mainnet
-- query link: https://dune.com/queries/232452


WITH copper_pools AS (
        SELECT
            "poolId" AS pool,
            'Copper' AS label
        FROM balancer_v2."Vault_evt_PoolRegistered" c
        INNER JOIN balancer_v2."LiquidityBootstrappingPoolFactory_call_create" cc
        ON c.evt_tx_hash = cc.call_tx_hash
        WHERE (LOWER(symbol) LIKE '%fla%' OR LOWER(symbol) LIKE 'grolbpt') 
        AND (LOWER(symbol) != '⚗️_fla'AND LOWER(symbol) != 'ankh_fla')
        AND cc.call_success
    ),
    
    element_pools AS (
        SELECT
            "poolId" AS pool,
            'Element' AS label
        FROM balancer_v2."Vault_evt_PoolRegistered" c
        INNER JOIN element."ConvergentCurvePoolFactory_call_create" cc
        ON c.evt_tx_hash = cc.call_tx_hash
        AND cc.call_success
    ),
    
    manual_labels AS (
        SELECT '\xc697051d1c6296c24ae3bcef39aca743861d9a81'::bytea AS pool, 'Aave' AS label
        UNION ALL 
        SELECT '\x32296969ef14eb0c6d29669c550d4a0449130230000200000000000000000080'::bytea AS pool, 'Lido' AS label
        UNION ALL
        SELECT '\x702605f43471183158938c1a3e5f5a359d7b31ba00020000000000000000009f'::bytea AS pool, 'Gro' AS label
    ),
    
    automatic_labels AS (
        SELECT * FROM copper_pools
        UNION ALL
        SELECT * FROM element_pools
    ),
    
    pool_labels AS (
        SELECT * FROM manual_labels
        UNION ALL
        SELECT * FROM automatic_labels
    )

SELECT day, COALESCE(label, 'Others') AS label, SUM(liquidity) AS tvl
FROM balancer.view_pools_liquidity b
LEFT JOIN pool_labels l
ON l.pool = b.pool
GROUP BY 1, 2
