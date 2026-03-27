-- part of a query repo
-- query name: TVL by chain, last 30 days
-- query link: https://dune.com/queries/3590605


SELECT day, blockchain, sum(protocol_liquidity_usd) AS TVL FROM balancer.liquidity
WHERE day > now () - interval '30' day
GROUP BY 1,2
ORDER BY 1 ASC, 3 DESC
