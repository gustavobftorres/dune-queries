-- part of a query repo
-- query name: Balancer Pool Liquidity
-- query link: https://dune.com/queries/4537952



SELECT 
    day,
    SUM(pool_liquidity_eth) AS tvl_eth,
    SUM(pool_liquidity_usd) AS tvl_usd
FROM query_4786688
WHERE (blockchain = '{{blockchain}}')
AND pool_address = {{balancer pool}}
AND day >= TIMESTAMP '{{start}}'
GROUP BY 1
ORDER BY 1 DESC