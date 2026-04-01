-- part of a query repo
-- query name: balancer <> CoW swaps vs. buffer balances
-- query link: https://dune.com/queries/4549627


WITH swaps AS(
SELECT 
    t.blockchain,
    block_time,
    version,
    tx_hash,
    CASE WHEN q.underlying_token = token_bought_address
    THEN token_bought_address
    WHEN q.underlying_token = token_sold_address
    THEN token_sold_address
    WHEN q.erc4626_token = token_bought_address
    THEN token_bought_address
    WHEN q.erc4626_token = token_sold_address
    THEN token_sold_address
    END AS token,
    q.underlying_token,
    underlying_token_symbol,
    erc4626_token_symbol,
    CASE WHEN q.underlying_token = token_bought_address
    THEN token_bought_amount
    WHEN q.underlying_token = token_sold_address
    THEN token_sold_amount
    WHEN q.erc4626_token = token_bought_address
    THEN token_bought_amount
    WHEN q.erc4626_token = token_sold_address
    THEN token_sold_amount
    END AS trade_amount,
    CASE WHEN q.underlying_token = token_bought_address
    THEN 'bought_underlying'
    WHEN q.underlying_token = token_sold_address
    THEN 'sold_underlying'
    WHEN q.erc4626_token = token_bought_address
    THEN 'bought_wrapped'
    WHEN q.erc4626_token = token_sold_address
    THEN 'bought_wrapped'    
    END AS trade_direction,
    q.underlying_balance
FROM dex.trades t
JOIN query_4549390 q ON q.evt_block_time <= t.block_time
AND t.block_time < q.time_of_next_change
AND t.blockchain = q.blockchain
AND (q.underlying_token = token_bought_address OR q.underlying_token = token_sold_address
OR q.erc4626_token = token_bought_address OR q.erc4626_token = token_sold_address)
WHERE project IN ('balancer')
AND tx_to = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41
AND t.version = '3'
AND block_date >= timestamp '2024-12-12')

SELECT 
    *,
    CASE WHEN underlying_balance > trade_amount
    THEN 1
    ELSE 0
    END AS swap_within_buffer
FROM swaps 
WHERE ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
AND ('{{erc4626_token}}' = 'All' OR erc4626_token_symbol = '{{erc4626_token}}')
ORDER BY block_time DESC