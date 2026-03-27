-- part of a query repo
-- query name: V1 vs. V2 TVL
-- query link: https://dune.com/queries/4106403


SELECT 
    day,
    version,
    SUM(protocol_liquidity_usd) AS tvl_usd,
    SUM(protocol_liquidity_eth) AS tvl_eth
FROM balancer.liquidity
WHERE day >= TIMESTAMP '{{start date}}'
AND day <= TIMESTAMP '{{end date}}'
GROUP BY 1, 2
ORDER BY 1 DESC