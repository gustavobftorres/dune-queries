-- part of a query repo
-- query name: Volume by DEX on Gnosis
-- query link: https://dune.com/queries/4046283


SELECT 
    DATE_TRUNC('month', block_date) AS month, 
    CASE WHEN (project='balancer') THEN 'Balancer' 
        ELSE 'Others' 
        END AS project, 
    SUM(amount_usd) AS volume
FROM dex.trades
WHERE blockchain = 'gnosis'
AND block_date > NOW() - INTERVAL '1' year
GROUP BY 1, 2