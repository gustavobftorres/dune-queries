-- part of a query repo
-- query name: DEX Capital Efficiency
-- query link: https://dune.com/queries/3628820


WITH swaps AS(
SELECT block_date, project, sum(amount_usd) AS volume
FROM dex.trades t
LEFT JOIN labels.balancer_v2_pools l ON l.address = t.project_contract_address AND l.blockchain = t.blockchain
WHERE project IN ('balancer')
AND block_date >= TIMESTAMP '{{Start Date}}'
AND ('{{Balancer Pool Type}}' = 'All' OR l.pool_type = '{{Balancer Pool Type}}')
AND ('{{Balancer Blockchain}}' = 'All' OR t.blockchain = '{{Balancer Blockchain}}')
GROUP BY 1, 2

UNION ALL

SELECT block_date, t.project, sum(amount_usd)
FROM dex.trades t
LEFT JOIN query_3629980 l 
ON l.pool = t.project_contract_address AND l.blockchain = t.blockchain AND l.project = t.project
WHERE t.project IN ('aerodrome', 'velodrome')
AND block_date >= TIMESTAMP '{{Start Date}}'
AND ('{{Aero/Velo Pool Type}}' = 'All' OR l.pool_type = '{{Balancer Pool Type}}')
GROUP BY 1, 2),

liquidity AS(
SELECT 
    day,
    'balancer' AS project,
    SUM(protocol_liquidity_usd) AS liquidity
    FROM balancer.liquidity
    WHERE day >= TIMESTAMP '{{Start Date}}'
    AND ('{{Balancer Pool Type}}' = 'All' OR pool_type = '{{Balancer Pool Type}}')
    AND ('{{Balancer Blockchain}}' = 'All' OR blockchain = '{{Balancer Blockchain}}')
    GROUP BY 1,2

UNION ALL

SELECT
    day,
    'aerodrome' AS project,
    SUM(liquidity_usd) AS liquidity
    FROM query_3625769
    WHERE day <= (SELECT MAX(day) FROM balancer.liquidity)
    AND day >= TIMESTAMP '{{Start Date}}'
    GROUP BY 1,2
    
UNION ALL

SELECT
    day,
    'velodrome' AS project,
    SUM(liquidity_usd) AS liquidity
    FROM query_3625994
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