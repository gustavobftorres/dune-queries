-- part of a query repo
-- query name: Daily TVL on Optimism
-- query link: https://dune.com/queries/3796474


SELECT CAST(day AS TIMESTAMP) AS day, SUM(protocol_liquidity_usd) AS tvl
FROM balancer.liquidity
WHERE blockchain = 'optimism'
AND day <= TIMESTAMP '{{End date}}'
AND day >= TIMESTAMP '{{Start date}}'
GROUP BY 1