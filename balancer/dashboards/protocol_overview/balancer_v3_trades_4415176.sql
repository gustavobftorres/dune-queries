-- part of a query repo
-- query name: balancer_v3_trades
-- query link: https://dune.com/queries/4415176


SELECT *
FROM dex.trades
WHERE project = 'balancer'
AND version = '3'
ORDER BY amount_usd NULLS FIRST