-- part of a query repo
-- query name: LST / LRT DEX 7 days volume
-- query link: https://dune.com/queries/3950019


SELECT
    SUM(amount_usd)/1e9 AS million_volume
FROM dex.trades t
INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
WHERE block_time > now() - interval '7' day
    AND ('{{Token Pair}}' = 'All' OR token_pair = '{{Token Pair}}')