-- part of a query repo
-- query name: Balancer Token Pairs
-- query link: https://dune.com/queries/4607004


SELECT
    DISTINCT token_pair, SUM(amount_usd)
FROM dex.trades
WHERE project = 'balancer'
GROUP BY 1

UNION

SELECT 
    'All', 1e16

ORDER BY 2 DESC