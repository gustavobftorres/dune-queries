-- part of a query repo
-- query name: WETH <> Token Swap Volume
-- query link: https://dune.com/queries/4216221


SELECT
    blockchain,
    token_pair,
    SUM(amount_usd) AS swap_volume,
    COUNT(*) AS total_swaps,
    APPROX_PERCENTILE(amount_usd, 0.25) AS percentile_25_swap_amount,
    APPROX_PERCENTILE(amount_usd, 0.5) AS median_swap_amount,
    APPROX_PERCENTILE(amount_usd, 0.75) AS percentile_75_swap_amount,
    AVG(amount_usd) AS average_swap_amount,
    SUM(CASE WHEN amount_usd < 25000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_swaps_under_25k,
    SUM(CASE WHEN amount_usd < 50000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_swaps_under_50k,
    SUM(CASE WHEN amount_usd < 100000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_swaps_under_100k
FROM balancer.trades
WHERE ((token_pair LIKE '%WETH%'
    AND (token_bought_symbol IN ('USDC', 'USDT', 'DAI', 'GYD', 'GHO', 'mUSD', 'sUSD', 'rETH', 'wstETH', 'ezETH')
         OR token_sold_symbol IN ('USDC', 'USDT', 'DAI', 'GYD', 'GHO', 'mUSD', 'sUSD', 'rETH', 'wstETH', 'ezETH')))
    OR (token_bought_symbol IN ('USDC', 'USDT', 'DAI')
         AND token_sold_symbol IN ('USDC', 'USDT', 'DAI')))
  AND ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
  AND token_pair != 'bb-a-WETH-wstETH'
  AND block_month >= TIMESTAMP '{{Start Date}}'
  AND block_month <= TIMESTAMP '{{End Date}}'
GROUP BY 1, 2
ORDER BY swap_volume DESC
