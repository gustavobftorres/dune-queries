-- part of a query repo
-- query name: WETH <> Stablecoin Monthly Swap Volume
-- query link: https://dune.com/queries/4216295


SELECT
    block_month,
    blockchain,
    CONCAT(SUBSTRING(blockchain, 1, 3), ': ',
    token_pair) AS token_pair,
    SUM(amount_usd) AS swap_volume,
    APPROX_PERCENTILE(amount_usd, 0.5) AS median_swap_amount,
    AVG(amount_usd) AS average_swap_amount
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