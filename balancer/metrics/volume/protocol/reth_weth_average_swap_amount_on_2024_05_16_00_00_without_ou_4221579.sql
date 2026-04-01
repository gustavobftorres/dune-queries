-- part of a query repo
-- query name: rETH - WETH average swap amount on 2024-05-16 00:00 without outliers
-- query link: https://dune.com/queries/4221579


SELECT 
    AVG(amount_usd) AS average, 
    APPROX_PERCENTILE(amount_usd, 0.5) AS median
FROM balancer.trades
WHERE token_pair = 'rETH-WETH'
AND block_date = TIMESTAMP '2024-05-16 00:00'
AND tx_hash NOT IN (0x79ba1fa0e2d56f2c04882122317de7d25f5db5202d47e583a0db0470764e31c7, 0x2a662315f2f13e90d7b67fbf4617a4b686239f2d202c5f549aa4d5248fd9bcdc)