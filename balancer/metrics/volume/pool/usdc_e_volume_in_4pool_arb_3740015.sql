-- part of a query repo
-- query name: USDC.e volume in 4POOL (arb)
-- query link: https://dune.com/queries/3740015


SELECT 
    t1.block_date, 
    t1.usdce_volume,
    t2.pool_volume,
    t1.usdce_volume / t2.pool_volume AS usdce_weight
FROM (
SELECT 
    block_date, 
    SUM(amount_usd) AS usdce_volume
FROM balancer.trades 
WHERE pool_id = 0x423a1323c871abc9d89eb06855bf5347048fc4a5000000000000000000000496
AND blockchain = 'arbitrum'
AND (token_bought_address = 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8 OR
token_sold_address = 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8)
GROUP BY 1) t1
LEFT JOIN (
SELECT 
    block_date, 
    SUM(amount_usd) AS pool_volume
FROM balancer.trades 
WHERE pool_id = 0x423a1323c871abc9d89eb06855bf5347048fc4a5000000000000000000000496
AND blockchain = 'arbitrum'
GROUP BY 1
) t2 ON t1.block_date = t2.block_date
ORDER BY 1 DESC