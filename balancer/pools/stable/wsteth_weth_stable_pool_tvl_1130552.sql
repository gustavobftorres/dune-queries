-- part of a query repo
-- query name: wstETH/WETH Stable Pool TVL
-- query link: https://dune.com/queries/1130552


SELECT day, SUM(usd_amount) AS "TVL"
FROM balancer_v2.view_liquidity
WHERE ('{{1. Pool ID}}' = 'All'
OR pool_id = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea)
GROUP BY 1
