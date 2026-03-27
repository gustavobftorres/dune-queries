-- part of a query repo
-- query name: USDC.e volume in Balancer vs. other DEXs
-- query link: https://dune.com/queries/3740061


SELECT 
    t1.block_date, 
    t1.balancer_volume,
    t2.dexs_volume,
    t1.balancer_volume / t2.dexs_volume AS balancer_weight
FROM (
SELECT 
    block_date, 
    SUM(amount_usd) AS balancer_volume
FROM dex.trades 
WHERE blockchain = 'arbitrum'
AND (token_bought_address = 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8 OR
token_sold_address = 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8)
AND project = 'balancer'
GROUP BY 1) t1
LEFT JOIN (
SELECT 
    block_date, 
    SUM(amount_usd) AS dexs_volume
FROM dex.trades 
WHERE blockchain = 'arbitrum'
AND (token_bought_address = 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8 OR
token_sold_address = 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8)
GROUP BY 1
) t2 ON t1.block_date = t2.block_date
/*WHERE pool_id = 0x423a1323c871abc9d89eb06855bf5347048fc4a5000000000000000000000496
AND blockchain = 'arbitrum'
AND (token_bought_address = 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8 OR
token_sold_address = 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8)*/
--GROUP BY 1, 3
ORDER BY 1 DESC