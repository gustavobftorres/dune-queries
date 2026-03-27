-- part of a query repo
-- query name: Balancer CoWSwap AMM Pool Selected
-- query link: https://dune.com/queries/3965068


WITH labels AS (
        SELECT * FROM (SELECT
            address,
            upper (name) as name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.balancer_cowswap_amm_pool
        WHERE blockchain = '{{4. Blockchain}}'
        GROUP BY 1, 2) l
        WHERE num = 1
    )
    
SELECT name
FROM labels 
WHERE address = {{1. Pool Address}}