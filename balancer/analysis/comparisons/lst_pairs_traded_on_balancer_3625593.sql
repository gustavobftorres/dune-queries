-- part of a query repo
-- query name: LST pairs traded on balancer
-- query link: https://dune.com/queries/3625593


SELECT * FROM(
SELECT token_pair, sum(amount_usd) 
FROM dex.trades t
INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
WHERE project = 'balancer'
AND t.blockchain IN ('base', 'optimism')
GROUP BY 1

UNION ALL

SELECT 'All', 1e20)

ORDER BY 2 DESC

