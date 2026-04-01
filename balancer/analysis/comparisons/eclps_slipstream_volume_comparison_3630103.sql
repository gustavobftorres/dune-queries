-- part of a query repo
-- query name: ECLPs / Slipstream Volume Comparison
-- query link: https://dune.com/queries/3630103


SELECT block_date, project, sum(amount_usd)
FROM dex.trades t
LEFT JOIN labels.balancer_v2_pools l ON l.address = t.project_contract_address AND l.blockchain = t.blockchain
WHERE project IN ('balancer')
AND block_date >= TIMESTAMP '{{Start Date}}'
AND (l.pool_type = 'ECLP')
AND ('{{Balancer Blockchain}}' = 'All' OR t.blockchain = '{{Balancer Blockchain}}')
GROUP BY 1, 2

UNION ALL

SELECT day, 'velodrome' AS project, sum(amount_usd)
FROM query_3630153 t
WHERE day >= TIMESTAMP '{{Start Date}}'
GROUP BY 1, 2