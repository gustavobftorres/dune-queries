-- part of a query repo
-- query name: Balancer V3 Liquidity
-- query link: https://dune.com/queries/4373500


SELECT
    day,
    SUM(protocol_liquidity_usd) AS tvl_usd,
    SUM(protocol_liquidity_eth) AS tvl_eth
FROM balancer.liquidity--query_4428144
WHERE version = '3'
AND ('{{blockchain}}' = 'All' or blockchain = '{{blockchain}}')
GROUP BY 1
ORDER BY 1 DESC