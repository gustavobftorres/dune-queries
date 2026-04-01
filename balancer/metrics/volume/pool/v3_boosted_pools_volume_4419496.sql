-- part of a query repo
-- query name: V3 Boosted Pools Volume
-- query link: https://dune.com/queries/4419496


SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 1 AS rn FROM balancer.trades s
INNER JOIN query_4419172 m ON s.project_contract_address = m.address
AND s.blockchain = m.blockchain
WHERE block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '1' DAY 
AND ('{{blockchain}}' = 'All' OR s.blockchain = '{{blockchain}}')
AND (version = '3')
UNION ALL
SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 2 AS rn FROM balancer.trades s 
INNER JOIN query_4419172 m ON s.project_contract_address = m.address
AND s.blockchain = m.blockchain
WHERE block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '7' DAY
AND ('{{blockchain}}' = 'All' OR s.blockchain = '{{blockchain}}')
AND (version = '3')
UNION ALL
SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 3 AS rn FROM balancer.trades s
INNER JOIN query_4419172 m ON s.project_contract_address = m.address
AND s.blockchain = m.blockchain
WHERE block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '30' DAY
AND ('{{blockchain}}' = 'All' OR s.blockchain = '{{blockchain}}')
AND (version = '3')
UNION ALL
SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 4 AS rn FROM balancer.trades s
INNER JOIN query_4419172 m ON s.project_contract_address = m.address
AND s.blockchain = m.blockchain
WHERE ('{{blockchain}}' = 'All' OR s.blockchain = '{{blockchain}}')
AND (version = '3')
ORDER BY rn ASC