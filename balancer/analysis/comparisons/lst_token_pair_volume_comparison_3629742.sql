-- part of a query repo
-- query name: LST Token Pair Volume Comparison
-- query link: https://dune.com/queries/3629742


SELECT project, token_pair, sum(amount_usd) AS volume_usd
FROM dex.trades t
INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
WHERE project IN ('balancer')
AND block_date >= TIMESTAMP '{{Start Date}}'
AND (t.blockchain IN ('optimism', 'base'))
AND ('{{Token Pair}}' = 'All' OR t.token_pair = '{{Token Pair}}')
GROUP BY 1, 2

UNION ALL

SELECT project, token_pair, sum(amount_usd) AS volume_usd
FROM dex.trades t
INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
WHERE project IN ('aerodrome', 'velodrome')
AND block_date >= TIMESTAMP '{{Start Date}}'
AND ('{{Token Pair}}' = 'All' OR t.token_pair = '{{Token Pair}}')
GROUP BY 1, 2
ORDER BY 3 DESC