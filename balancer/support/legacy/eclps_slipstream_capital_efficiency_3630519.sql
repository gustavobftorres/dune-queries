-- part of a query repo
-- query name: ECLPs / Slipstream Capital Efficiency
-- query link: https://dune.com/queries/3630519


WITH swaps AS(
    SELECT block_date, project, sum(amount_usd) AS volume
    FROM balancer.trades t
    LEFT JOIN labels.balancer_v2_pools l ON l.address = t.project_contract_address AND l.blockchain = t.blockchain
    WHERE project IN ('balancer')
    AND block_date >= TIMESTAMP '{{Start Date}}'
    AND (l.pool_type = 'ECLP')
    AND ('{{Balancer Blockchain}}' = 'All' OR t.blockchain = '{{Balancer Blockchain}}')
--    AND pool_id NOT IN (0x2191df821c198600499aa1f0031b1a7514d7a7d9000200000000000000000639, 0xf01b0684c98cd7ada480bfdf6e43876422fa1fc10002000000000000000005de)
    GROUP BY 1, 2
    HAVING sum(amount_usd) > 1e5
    
    UNION ALL
    
    SELECT day, 'velodrome' AS project, sum(amount_usd)
    FROM query_3630153 t
    WHERE day >= TIMESTAMP '{{Start Date}}'
    GROUP BY 1, 2),

liquidity AS(
SELECT 
    day,
    'balancer' AS project,
    SUM(protocol_liquidity_usd) AS liquidity
    FROM balancer.liquidity
    WHERE day >= TIMESTAMP '{{Start Date}}'
    AND ('{{Balancer Blockchain}}' = 'All' OR blockchain = '{{Balancer Blockchain}}')
    AND (pool_type = 'ECLP')
    AND pool_id NOT IN (0x2191df821c198600499aa1f0031b1a7514d7a7d9000200000000000000000639, 0xf01b0684c98cd7ada480bfdf6e43876422fa1fc10002000000000000000005de)
    GROUP BY 1,2

UNION ALL

SELECT
    day,
    'velodrome' AS project,
    SUM(liquidity_usd) AS liquidity
    FROM query_3630446
    WHERE day <= (SELECT MAX(day) FROM balancer.liquidity)
    AND day >= TIMESTAMP '{{Start Date}}'
    GROUP BY 1,2
)

    SELECT 
        s.block_date,
        s.project,
        s.volume/l.liquidity AS capital_efficiency
    FROM swaps s
    LEFT JOIN liquidity l ON s.block_date = l.day
    AND l.project = s.project
    WHERE s.volume > 0