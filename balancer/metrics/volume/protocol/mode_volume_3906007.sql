-- part of a query repo
-- query name: Mode Volume
-- query link: https://dune.com/queries/3906007


SELECT SUM(amount_usd)/ 1e3 AS "Volume on Balancer", 1 AS rn FROM dune.balancer.dataset_mode_snapshots 
WHERE CAST(day AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '1' DAY 
UNION ALL
SELECT SUM(amount_usd)/ 1e3 AS "Volume on Balancer", 2 AS rn FROM dune.balancer.dataset_mode_snapshots
WHERE CAST(day AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '7' DAY
UNION ALL
SELECT SUM(amount_usd)/ 1e6 AS "Volume on Balancer", 3 AS rn FROM dune.balancer.dataset_mode_snapshots
WHERE CAST(day AS TIMESTAMP) >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY rn ASC