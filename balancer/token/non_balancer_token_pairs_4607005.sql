-- part of a query repo
-- query name: Non-Balancer Token Pairs
-- query link: https://dune.com/queries/4607005


SELECT
    DISTINCT token_pair, SUM(amount_usd)
FROM dex.trades
GROUP BY 1

UNION

SELECT 
    'All', 1e20

ORDER BY 2 DESC