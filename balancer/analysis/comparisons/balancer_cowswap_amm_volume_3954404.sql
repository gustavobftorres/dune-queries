-- part of a query repo
-- query name: Balancer CoWSwap AMM Volume
-- query link: https://dune.com/queries/3954404


SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 1 AS rn FROM balancer_cowswap_amm.trades 
WHERE block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '1' DAY 
AND ('{{2. Blockchain}}' = 'All' OR blockchain = '{{2. Blockchain}}')
UNION ALL
SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 2 AS rn FROM balancer_cowswap_amm.trades 
WHERE block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '7' DAY
AND ('{{2. Blockchain}}' = 'All' OR blockchain = '{{2. Blockchain}}')
UNION ALL
SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 3 AS rn FROM balancer_cowswap_amm.trades 
WHERE block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '30' DAY
AND ('{{2. Blockchain}}' = 'All' OR blockchain = '{{2. Blockchain}}')
ORDER BY rn ASC