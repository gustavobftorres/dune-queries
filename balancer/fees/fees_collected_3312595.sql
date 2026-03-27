-- part of a query repo
-- query name: Fees Collected
-- query link: https://dune.com/queries/3312595


SELECT SUM(protocol_fee_collected_usd) AS "Revenue on Balancer", 1 AS rn FROM balancer.protocol_fee
WHERE day >= CAST(NOW() AS TIMESTAMP) - INTERVAL '1' DAY 
AND ('{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}')
UNION ALL
SELECT SUM(protocol_fee_collected_usd)/1e3 AS "Revenue on Balancer", 2 AS rn FROM balancer.protocol_fee
WHERE day >= CAST(NOW() AS TIMESTAMP) - INTERVAL '7' DAY
AND ('{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}')
UNION ALL
SELECT SUM(protocol_fee_collected_usd)/1e3 AS "Revenue on Balancer", 3 AS rn FROM balancer.protocol_fee
WHERE day >= CAST(NOW() AS TIMESTAMP) - INTERVAL '30' DAY
AND ('{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}')
ORDER BY rn ASC