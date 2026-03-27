-- part of a query repo
-- query name: ECLPs / Slipstream  TVL Comparison
-- query link: https://dune.com/queries/3630216


SELECT 
    day,
    'balancer' AS project,
    CASE WHEN '{{Currency}}' = 'eth'
    THEN SUM(protocol_liquidity_eth)
    ELSE SUM(protocol_liquidity_usd)
    END AS liquidity
    FROM balancer.liquidity
    WHERE day >= TIMESTAMP '{{Start Date}}'
    AND ('{{Balancer Blockchain}}' = 'All' OR blockchain = '{{Balancer Blockchain}}')
    AND (pool_type = 'ECLP')
    GROUP BY 1,2

UNION ALL

SELECT
    day,
    'velodrome' AS project,
    CASE WHEN '{{Currency}}' = 'eth'
    THEN SUM(liquidity_eth)
    ELSE SUM(liquidity_usd)
    END AS liquidity
    FROM query_3630446
    WHERE day <= (SELECT MAX(day) FROM balancer.liquidity)
    AND day >= TIMESTAMP '{{Start Date}}'
    GROUP BY 1,2