-- part of a query repo
-- query name: Balancer Exchange
-- query link: https://dune.com/queries/31200


WITH swaps AS (
        SELECT  
            date_trunc('week', block_time) AS week,
            COUNT(*) AS txns,
            SUM(usd_amount) AS volume
        FROM dex."trades"
        WHERE project = 'Balancer'
        GROUP BY 1
)

SELECT 
    week, 
    txns AS "Swaps", 
    volume AS "Volume",
    volume/txns AS "Avg. Volume per Swap"
FROM swaps