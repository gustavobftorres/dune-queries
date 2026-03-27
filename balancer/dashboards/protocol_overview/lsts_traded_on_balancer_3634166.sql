-- part of a query repo
-- query name: LSTs traded on Balancer
-- query link: https://dune.com/queries/3634166


SELECT 
    t.project,
    l.symbol,
    SUM(CASE WHEN block_time >= now() - interval '30' day THEN amount_usd ELSE 0 END) AS volume_30d,
    SUM(CASE WHEN block_time >= now() - interval '7' day THEN amount_usd ELSE 0 END) AS volume_7d,
    SUM(CASE WHEN block_time >= now() - interval '24' hour THEN amount_usd ELSE 0 END) AS volume_1d
FROM balancer.trades t
INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
WHERE project IN ('balancer')
AND t.blockchain IN ('base', 'optimism') 
GROUP BY 1, 2

UNION ALL

SELECT
    'balancer',
    'All',
    1e12,
    1e12,
    1e12
ORDER BY 3 DESC
