-- part of a query repo
-- query name: V3 Liquidity
-- query link: https://dune.com/queries/4372452


SELECT 
    SUM(protocol_liquidity_usd) / 1e6 AS tvl_usd,
    SUM(protocol_liquidity_eth) AS tvl_eth
FROM balancer.liquidity
WHERE day = CURRENT_DATE
AND version = '3'
AND ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')