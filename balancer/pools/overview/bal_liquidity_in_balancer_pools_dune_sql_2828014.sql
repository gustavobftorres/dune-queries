-- part of a query repo
-- query name: BAL Liquidity in Balancer Pools (Dune SQL)
-- query link: https://dune.com/queries/2828014


SELECT CAST(day as timestamp) as day, SUM(token_balance) AS "BAL Liquidity"
FROM balancer.liquidity
WHERE token_symbol = 'BAL'
GROUP BY 1