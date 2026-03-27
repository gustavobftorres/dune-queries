-- part of a query repo
-- query name: Balancer V3 Volume Distribution (Dune SQL)
-- query link: https://dune.com/queries/4353343


WITH swaps AS (
        SELECT 
            version,
            amount_usd
        FROM balancer.trades
        WHERE block_time >= TIMESTAMP '2024-11-29 00:00'
        AND ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
        AND version = '3'
    )
    
SELECT
    version,
    CASE 
        WHEN (amount_usd) BETWEEN 0 AND 100 THEN '< 100' 
        WHEN (amount_usd) BETWEEN 100 AND 1000 THEN '< 1K' 
        WHEN (amount_usd) BETWEEN 1000 AND 10000 THEN '< 10K' 
        WHEN (amount_usd) BETWEEN 10000 AND 100000 THEN '< 100K' 
        WHEN (amount_usd) BETWEEN 100000 AND 1000000 THEN '< 1M' 
    END AS volume,
    CASE 
        WHEN (amount_usd) BETWEEN 0 AND 100 THEN '1' 
        WHEN (amount_usd) BETWEEN 100 AND 1000 THEN '2' 
        WHEN (amount_usd) BETWEEN 1000 AND 10000 THEN '3' 
        WHEN (amount_usd) BETWEEN 10000 AND 100000 THEN '4' 
        WHEN (amount_usd) BETWEEN 100000 AND 1000000 THEN '5' 
    END AS n,
    COUNT(amount_usd) AS n_trades
FROM swaps
GROUP BY 1, 2, 3
ORDER BY 3