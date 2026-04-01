-- part of a query repo
-- query name: WETH <> Stablecoin Swap Analysis
-- query link: https://dune.com/queries/4216715


WITH swaps AS(
SELECT
    CASE WHEN '{{Aggregation}}' = 'Daily'
    THEN block_date
    WHEN '{{Aggregation}}' = 'Weekly'
    THEN DATE_TRUNC('week', block_date)
    WHEN '{{Aggregation}}' = 'Monthly'
    THEN block_month
    END AS block_date,
    blockchain,
    token_pair,
    CASE WHEN token_bought_symbol IN 
    ('USDC', 'USDT', 'DAI', 'GYD', 'GHO', 'mUSD', 'sUSD', 'rETH', 'wstETH', 'ezETH')
    THEN token_bought_symbol
    WHEN token_sold_symbol IN
    ('USDC', 'USDT', 'DAI', 'GYD', 'GHO', 'mUSD', 'sUSD', 'rETH', 'wstETH', 'ezETH')
    THEN token_sold_symbol
    END AS coin,
    SUM(amount_usd) AS swap_volume,
    APPROX_PERCENTILE(amount_usd, 0.5) AS median_swap_amount,
    AVG(amount_usd) AS average_swap_amount,
    COUNT(*) AS number_of_swaps
FROM balancer.trades
    WHERE ((token_pair LIKE '%WETH%'
      AND (token_bought_symbol IN ('USDC', 'USDT', 'DAI', 'GYD', 'GHO', 'mUSD', 'sUSD', 'rETH', 'wstETH', 'ezETH')
           OR token_sold_symbol IN ('USDC', 'USDT', 'DAI', 'GYD', 'GHO', 'mUSD', 'sUSD', 'rETH', 'wstETH', 'ezETH')
           ))
           OR (token_bought_symbol IN ('USDC', 'USDT', 'DAI')
           AND token_sold_symbol IN ('USDC', 'USDT', 'DAI')))
AND ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
AND block_month >= TIMESTAMP '{{Start Date}}'
AND block_month <= TIMESTAMP '{{End Date}}'
GROUP BY 1, 2, 3, 4)

SELECT
    *
FROM swaps
WHERE coin = '{{Coin}}'
ORDER BY 1 DESC, 5 DESC