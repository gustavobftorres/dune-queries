-- part of a query repo
-- query name: Buffer Balances vs. Trade Amount Distribution
-- query link: https://dune.com/queries/4549881


SELECT
    t.blockchain,
    q.erc4626_token_symbol,    
    q.underlying_balance,
    AVG(trade_amount) AS avg_trade_amount,
    APPROX_PERCENTILE(trade_amount, 0.25) AS p25_trade_amount,
    APPROX_PERCENTILE(trade_amount, 0.50) AS p50_trade_amount,
    APPROX_PERCENTILE(trade_amount, 0.75) AS p75_trade_amount,
    APPROX_PERCENTILE(trade_amount, 0.90) AS p90_trade_amount,
    APPROX_PERCENTILE(trade_amount, 0.95) AS p95_trade_amount,
    COUNT(*) AS num_trades,
    MIN(trade_amount) AS min_trade_amount,
    MAX(trade_amount) AS max_trade_amount,
    CASE 
        WHEN q.underlying_balance > APPROX_PERCENTILE(trade_amount, 0.95) THEN 'p95'
        WHEN q.underlying_balance > APPROX_PERCENTILE(trade_amount, 0.90) THEN 'p90'
        WHEN q.underlying_balance > APPROX_PERCENTILE(trade_amount, 0.75) THEN 'p75'
        WHEN q.underlying_balance > APPROX_PERCENTILE(trade_amount, 0.50) THEN 'p50'
        WHEN q.underlying_balance > APPROX_PERCENTILE(trade_amount, 0.25) THEN 'p25'
        ELSE 'N/A'
    END AS highest_percentile_within_buffer 
FROM query_4717948 t
LEFT JOIN query_4549390 q ON q.underlying_token = t.underlying_token
AND q.blockchain = t.blockchain
AND q.erc4626_token_symbol = t.erc4626_token_symbol
AND q.rn = 1
WHERE ('{{blockchain}}' = 'All' OR q.blockchain = '{{blockchain}}')
AND ('{{erc4626_token}}' = 'All' OR q.erc4626_token_symbol = '{{erc4626_token}}')
GROUP BY 1, 2, 3
ORDER BY 3 DESC