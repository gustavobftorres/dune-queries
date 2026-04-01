-- part of a query repo
-- query name: WETH <> Stablecoin Daily Swap Volume
-- query link: https://dune.com/queries/4216707


SELECT
    block_date,
    blockchain,
    CONCAT(SUBSTRING(blockchain, 1, 3), ': ',
    token_pair) AS token_pair,
    SUM(amount_usd) AS swap_volume,
    APPROX_PERCENTILE(amount_usd, 0.5) AS median_swap_amount,
    AVG(amount_usd) AS average_swap_amount,
    MIN(amount_usd) AS min_swap_amount,
    MAX(amount_usd) AS max_swap_amount,
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
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC