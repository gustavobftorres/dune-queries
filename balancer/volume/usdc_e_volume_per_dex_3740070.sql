-- part of a query repo
-- query name: USDC.e volume per DEX
-- query link: https://dune.com/queries/3740070


SELECT 
    block_date, 
    project,
    SUM(amount_usd) AS usdce_volume
FROM dex.trades 
WHERE blockchain = 'arbitrum'
AND (token_bought_address = 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8 OR
token_sold_address = 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8)
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC