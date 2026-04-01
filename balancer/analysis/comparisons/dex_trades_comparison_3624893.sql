-- part of a query repo
-- query name: DEX Trades Comparison
-- query link: https://dune.com/queries/3624893


SELECT block_date, project, sum(amount_usd)
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
AND ('{{Aero/Velo Pool Type}}' = 'All' OR l.pool_type = '{{Aero/Velo Pool Type}}')
GROUP BY 1, 2