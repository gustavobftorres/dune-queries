-- part of a query repo
-- query name: CoW Volume over DEXs on Gnosis
-- query link: https://dune.com/queries/4046322


SELECT 
    DATE_TRUNC('month', block_date) AS month, 
    CASE WHEN (project='balancer') THEN 'Balancer' 
        ELSE 'Others' 
        END AS project, 
    SUM(amount_usd) AS volume
FROM dex.trades 
WHERE tx_to IN (0x9008D19f58AAbD9eD0D60971565AA8510560ab41)
AND block_date > NOW() - interval '12' month
AND blockchain = 'gnosis'
group by 1, 2