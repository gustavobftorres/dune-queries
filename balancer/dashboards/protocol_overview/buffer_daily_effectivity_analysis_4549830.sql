-- part of a query repo
-- query name: Buffer Daily Effectivity Analysis
-- query link: https://dune.com/queries/4549830


SELECT
    DATE_TRUNC('day', block_time) AS block_date,
    blockchain,
    erc4626_token_symbol AS symbol,
    COUNT(*) AS total_swaps,
    SUM(swap_within_buffer) AS swaps_within_buffer,
    SUM(CAST(swap_within_buffer AS DOUBLE)) / COUNT(*) AS buffer_effectiveness,
    SUM(CASE WHEN swap_within_buffer = 1 THEN CAST(trade_amount AS DOUBLE) ELSE 0 END) / SUM(trade_amount) AS buffer_effectiveness_amount
FROM query_4717948
WHERE ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
AND ('{{erc4626_token}}' = 'All' OR erc4626_token_symbol = '{{erc4626_token}}')
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC