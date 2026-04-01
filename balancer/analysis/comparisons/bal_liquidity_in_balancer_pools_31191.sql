-- part of a query repo
-- query name: BAL Liquidity in Balancer Pools
-- query link: https://dune.com/queries/31191


SELECT day, SUM(cumulative_amount )/1e18 AS "BAL Liquidity"
FROM balancer.view_balances
WHERE token = '\xba100000625a3754423978a60c9317c58a424e3d'
GROUP BY 1