-- part of a query repo
-- query name: Sandwich Attacks - TXs
-- query link: https://dune.com/queries/4532901


SELECT
    t1.blockchain,
    t1.version,
    t1.block_time,
    t1.block_number,
    t1.token_bought_symbol,
    t1.token_sold_symbol,
    t1.token_bought_amount,
    t1.token_sold_amount,
    t1.amount_usd,
    t1.project_contract_address,
    t1.pool_symbol,
    t1.pool_type,
    t1.tx_from,
    t1.tx_to,
    t1.tx_hash,
    t1.evt_index
FROM balancer.trades t1
INNER JOIN dex.sandwiches t2 
    ON t1.tx_hash = t2.tx_hash
WHERE ('{{version}}' = 'All' OR t1.version = '{{version}}')
  AND ('{{blockchain}}' = 'All' OR t1.blockchain = '{{blockchain}}')
  AND ('{{pool_address}}' = 'All' OR CAST(t1.project_contract_address AS VARCHAR) = '{{pool_address}}')
  AND block_date >= TIMESTAMP '{{start}}'
ORDER BY block_time DESC
