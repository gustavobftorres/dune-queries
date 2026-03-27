-- part of a query repo
-- query name: LST / LRT DEX 24 hours volume
-- query link: https://dune.com/queries/3950016


SELECT
  SUM(CAST(amount_usd AS DOUBLE)) / 1e6 AS million_volume
FROM
  dex."trades" AS t
INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
WHERE
  block_time > NOW() - INTERVAL '24' hour
    AND ('{{Token Pair}}' = 'All' OR token_pair = '{{Token Pair}}')