-- part of a query repo
-- query name: Balancer V2 TVL by Pool
-- query link: https://dune.com/queries/134670


WITH labels AS (
        SELECT * FROM (SELECT
            address,
            name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.labels
        WHERE "type" IN ('balancer_pool', 'balancer_v2_pool')
        GROUP BY 1, 2) l
        WHERE num = 1
    ),
    
    tvl AS (
        SELECT day, pool_id, SUM(usd_amount) AS tvl
        FROM balancer_v2.view_liquidity
        WHERE day <= '{{3. End date}}'
        AND day >= '{{2. Start date}}'
        AND ('{{1. Pool ID}}' = 'All'
        OR pool_id = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea)
        GROUP BY 1, 2
    ),
    
    total_tvl AS (
        SELECT day, 'Total' AS pool_id, SUM(tvl) AS tvl
        FROM tvl
        GROUP BY 1, 2
        ORDER BY 1 DESC
    ),
    
    top_pools AS (
        SELECT DISTINCT pool_id, tvl, CONCAT(SUBSTRING(UPPER(l.name), 0, 15), ' (', SUBSTRING(t.pool_id::text, 3, 8), ')') AS symbol
        FROM tvl t
        LEFT JOIN labels l ON l.address = SUBSTRING(t.pool_id::text, 0, 43)::bytea
        WHERE day = LEAST(CURRENT_DATE, '{{3. End date}}')
        AND day = CURRENT_DATE
        AND tvl IS NOT NULL
        ORDER BY 2 DESC, 3 DESC 
        LIMIT 5
    ),
    
    tvl_by_pool AS (
        SELECT t.day, COALESCE(p.symbol, 'Others') AS pool, 
            --CASE WHEN '{{1. Pool ID}}' = 'All' THEN NULL ELSE 
            SUM(t.tvl) --END 
            AS tvl
        FROM tvl t
        LEFT JOIN top_pools p ON p.pool_id = t.pool_id
        GROUP BY 1, 2
    )
SELECT p.*, t.tvl as protocol_tvl
FROM tvl_by_pool p
LEFT JOIN total_tvl t
ON t.day = p.day
WHERE p.day >= '{{2. Start date}}'
and p.day <= '{{3. End date}}'
order by 1 desc