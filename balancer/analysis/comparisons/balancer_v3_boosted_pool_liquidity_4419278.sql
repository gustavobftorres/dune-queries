-- part of a query repo
-- query name: Balancer V3 Boosted Pool Liquidity
-- query link: https://dune.com/queries/4419278


SELECT
    day,
    lending_market,
    SUM(protocol_liquidity_usd) AS tvl_usd,
    SUM(protocol_liquidity_eth) AS tvl_eth
FROM balancer.liquidity s
INNER JOIN query_4419172 m ON s.pool_address = m.address
AND s.blockchain = m.blockchain
WHERE version = '3'
AND ('{{blockchain}}' = 'All' or s.blockchain = '{{blockchain}}')
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC