-- part of a query repo
-- query name: ZBU/USDC pool daily volume
-- query link: https://dune.com/queries/4745031


SELECT 
    block_date, 
    SUM(amount_usd) AS swap_volume,
    SUM(SUM(amount_usd)) OVER (
        ORDER BY block_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS volume_30d
FROM balancer.trades 
WHERE pool_id = 0x59501a303b1bdf5217617745acec4d99107383f0000200000000000000000197
AND blockchain = 'base' 
AND block_date >= TIMESTAMP '{{start_date}}'
AND block_date <= TIMESTAMP '{{end_date}}'
GROUP BY 1
ORDER BY 1 DESC;
