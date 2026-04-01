-- part of a query repo
-- query name: Balancer TVL by Project on Mainnet
-- query link: https://dune.com/queries/261688


WITH projects AS (
        SELECT 
            name,
            address
        FROM labels.labels
        WHERE "type" = 'balancer_project'
        AND author IN ('balancerlabs', 'metacrypto', 'markusbkoch', 'mangool', 'astivelman')
    )

SELECT day, name, SUM(liquidity) AS tvl
FROM balancer.view_pools_liquidity l
JOIN projects p
ON p.address = SUBSTRING(l.pool, 0, 21)
GROUP BY 1, 2
