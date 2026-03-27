-- part of a query repo
-- query name: Volume
-- query link: https://dune.com/queries/2557257


SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 1 AS rn FROM dex.trades 
WHERE project = 'balancer' AND block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '1' DAY
UNION ALL
SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 2 AS rn FROM dex.trades 
WHERE project = 'balancer' AND block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '7' DAY
UNION ALL
SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 3 AS rn FROM dex.trades 
WHERE project = 'balancer' AND block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '30' DAY
ORDER BY rn ASC