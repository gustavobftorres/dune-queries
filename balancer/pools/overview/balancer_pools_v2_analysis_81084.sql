-- part of a query repo
-- query name: Balancer Pools V2 Analysis
-- query link: https://dune.com/queries/81084


WITH labels AS (
        SELECT * FROM (SELECT
            address,
            name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.labels
        WHERE "type" = 'balancer_v2_pool'
        GROUP BY 1, 2) l
        WHERE num = 1
    ),
    
    total_tvl AS (
        SELECT day, pool, SUM(liquidity) AS "TVL"
        FROM balancer.view_pools_liquidity
        GROUP BY 1, 2
    ),
    
    last_tvl AS(
        SELECT SUBSTRING(t.pool, 0, 21) AS pool, t."TVL" AS tvl
        FROM total_tvl t
        WHERE day = (select max(day) from total_tvl)
    )
    
SELECT 
    COALESCE(CONCAT(SUBSTRING(UPPER(l.name), 0, 16)), "poolId"::text) AS composition,
    tvl,
    CONCAT('<a href="https://duneanalytics.com/balancerlabs/Balancer-Pool-Analysis?1.%20Pool%20ID=0', SUBSTRING(r."poolId"::text, 2), '">view stats</a>') AS stats,
    CONCAT('<a target="_blank" href="https://app.balancer.fi/#/pool/0', SUBSTRING(r."poolId"::text, 2), '">balancer ↗</a>') AS pool,
    CONCAT('<a target="_blank" href="https://etherscan.io/address/0', SUBSTRING(pool::text, 2, 42), '">etherscan ↗</a>') AS etherscan
FROM last_tvl p
INNER JOIN balancer_v2."Vault_evt_PoolRegistered" r ON p.pool = r."poolAddress"
LEFT JOIN labels l ON l.address = p.pool
ORDER BY 2 DESC NULLS LAST