-- part of a query repo
-- query name: Balancer Pool Selected
-- query link: https://dune.com/queries/2718018


WITH labels AS (
        SELECT * FROM (SELECT
            address,
            upper (name) as name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.addresses
        WHERE category IN ('balancer_v1_pool', 'balancer_v2_pool', 'balancer_v3_pool')
        AND blockchain = '{{4. Blockchain}}'
        GROUP BY 1, 2) l
        WHERE num = 1
    )
    
SELECT name
FROM labels 
WHERE CAST(address as varchar) = SUBSTRING('{{1. Pool ID}}', 1, 42)