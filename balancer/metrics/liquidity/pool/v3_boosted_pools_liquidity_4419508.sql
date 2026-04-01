-- part of a query repo
-- query name: V3 Boosted Pools Liquidity
-- query link: https://dune.com/queries/4419508


SELECT 
    SUM(protocol_liquidity_usd) / 1e6 AS tvl_usd,
    SUM(protocol_liquidity_eth) AS tvl_eth
FROM balancer.liquidity l
INNER JOIN query_4419172 q ON l.pool_address = q.address
AND l.blockchain = q.blockchain
WHERE day = CURRENT_DATE - interval '1' day
AND version = '3'
AND ('{{blockchain}}' = 'All' OR l.blockchain = '{{blockchain}}')