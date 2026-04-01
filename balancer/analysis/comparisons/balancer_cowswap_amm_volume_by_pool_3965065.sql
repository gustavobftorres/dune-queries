-- part of a query repo
-- query name: Balancer CoWSwap AMM Volume by Pool
-- query link: https://dune.com/queries/3965065


SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 1 AS rn FROM balancer_cowswap_amm.trades 
WHERE block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '1' DAY AND ('{{1. Pool Address}}' = 'All' OR project_contract_address = {{1. Pool Address}}) 
AND blockchain = '{{4. Blockchain}}'
UNION ALL
SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 2 AS rn FROM balancer_cowswap_amm.trades 
WHERE block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '7' DAY AND ('{{1. Pool Address}}' = 'All' OR project_contract_address = {{1. Pool Address}})
UNION ALL
SELECT SUM(amount_usd)/1e6 AS "Volume on Balancer", 3 AS rn FROM balancer_cowswap_amm.trades 
WHERE block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '30' DAY AND ('{{1. Pool Address}}' = 'All' OR project_contract_address = {{1. Pool Address}})
ORDER BY rn ASC