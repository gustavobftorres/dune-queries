-- part of a query repo
-- query name: dex_trades testing
-- query link: https://dune.com/queries/188058


SELECT 
    date_trunc('week', block_time) AS week,
    COUNT(*) AS txns,
    SUM(usd_amount) AS volume
FROM dune_user_generated.dex_trades 
WHERE project = 'Balancer'
GROUP BY 1