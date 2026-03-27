-- part of a query repo
-- query name: Buffer Effectivity Analysis
-- query link: https://dune.com/queries/4551067


SELECT
    blockchain,
    erc4626_token_symbol AS symbol,
    COUNT(*) AS total_swaps,
    SUM(swap_within_buffer) AS swaps_within_buffer,
    SUM(CAST(swap_within_buffer AS DOUBLE)) / COUNT(*) AS buffer_effectiveness
FROM query_4549627
GROUP BY 1, 2
ORDER BY 3 DESC