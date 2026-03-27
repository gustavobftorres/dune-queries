-- part of a query repo
-- query name: 4 Pool (Arb) swaps by token pair
-- query link: https://dune.com/queries/3740107


SELECT 
    block_date, 
    token_pair,
    CASE WHEN (token_bought_address = 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8 OR token_sold_address = 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8)
     THEN TRUE
     ELSE FALSE
     END AS usdce_flag,
    SUM(amount_usd) AS usdce_volume
FROM balancer.trades 
WHERE pool_id = 0x423a1323c871abc9d89eb06855bf5347048fc4a5000000000000000000000496
AND blockchain = 'arbitrum'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC