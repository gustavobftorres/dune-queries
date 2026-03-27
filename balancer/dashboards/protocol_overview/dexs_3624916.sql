-- part of a query repo
-- query name: dexs
-- query link: https://dune.com/queries/3624916


SELECT project, sum(amount_usd) 
FROM dex.trades
WHERE project != 'balancer'
GROUP BY 1
ORDER BY 2 DESC