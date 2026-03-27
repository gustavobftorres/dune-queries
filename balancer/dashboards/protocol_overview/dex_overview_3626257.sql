-- part of a query repo
-- query name: DEX Overview
-- query link: https://dune.com/queries/3626257



SELECT 
    t.project,
    SUM(CASE WHEN block_time >= now() - interval '30' day THEN amount_usd ELSE 0 END) AS volume_30d,
    SUM(CASE WHEN block_time >= now() - interval '7' day THEN amount_usd ELSE 0 END) AS volume_7d,
    SUM(CASE WHEN block_time >= now() - interval '24' hour THEN amount_usd ELSE 0 END) AS volume_1d
FROM dex.trades t
INNER JOIN dune.balancer.result_lst_tokens l 
        ON t.blockchain = l.blockchain 
        AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
WHERE ('{{Blockchain}}' = 'All' OR t.blockchain = '{{Blockchain}}')
GROUP BY 1
