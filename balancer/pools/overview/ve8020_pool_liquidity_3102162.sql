-- part of a query repo
-- query name: ve8020 Pool Liquidity
-- query link: https://dune.com/queries/3102162


SELECT CAST(day as timestamp) as day, sum(pool_liquidity_usd) as tvl, 
sum(pool_liquidity_usd)/1e6 as short_tvl
FROM balancer.liquidity 
WHERE 
BYTEARRAY_SUBSTRING (pool_id,1,20) = {{Pool Address}}
AND blockchain = '{{Blockchain}}'
GROUP BY 1
ORDER BY 1 DESC