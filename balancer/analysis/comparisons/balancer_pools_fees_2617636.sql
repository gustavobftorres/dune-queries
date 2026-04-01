-- part of a query repo
-- query name: Balancer Pools Fees
-- query link: https://dune.com/queries/2617636


WITH fees AS(
    SELECT swap_fee_percentage/1e16 AS fee
    FROM balancer.pools_fees
    WHERE ('{{4. Blockchain}}' = 'All' or blockchain = '{{4. Blockchain}}')
    AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
    )

SELECT
    CASE 
        WHEN ("fee") BETWEEN 0 AND 0.25 THEN '< 0.25%' 
        WHEN ("fee") BETWEEN 0.25 AND 0.5 THEN '< 0.50%' 
        WHEN ("fee") BETWEEN 0.5 AND 1 THEN '< 1%' 
        WHEN ("fee") BETWEEN 1 AND 5 THEN '< 5%' 
        WHEN ("fee") BETWEEN 5 AND 10 THEN '< 10%' 
    END AS "fee",
    COUNT("fee") AS "Pools"
FROM fees
GROUP BY 1
ORDER BY 2 DESC