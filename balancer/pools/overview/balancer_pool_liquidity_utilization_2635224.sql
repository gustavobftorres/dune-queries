-- part of a query repo
-- query name: Balancer Pool Liquidity Utilization
-- query link: https://dune.com/queries/2635224


WITH 
    swaps AS (
        SELECT
            date_trunc('month', d.block_time) AS day,
            SUM(amount_usd) AS volume
        FROM balancer.trades d
        WHERE blockchain = '{{4. Blockchain}}'
        AND ('{{1. Pool ID}}' = 'All' OR CAST(project_contract_address as varchar) = SUBSTRING('{{1. Pool ID}}',1,42))
        GROUP BY 1
    ),

    total_tvl AS (
        SELECT date_trunc('month', day) AS day, SUM(pool_liquidity_usd) AS tvl
        FROM balancer.liquidity
        WHERE ('{{1. Pool ID}}' = 'All' OR
        SUBSTRING(CAST(pool_id as varchar), 1, 42) = SUBSTRING('{{1. Pool ID}}', 1,42))
        AND blockchain = '{{4. Blockchain}}'
        GROUP BY 1
    )
   
SELECT
    CAST(t.day as timestamp) as day,
    (s.volume)/(t.tvl) AS Ratio,
    s.volume,
    t.tvl
FROM total_tvl t
LEFT JOIN swaps s ON s.day = t.day
WHERE t.day >= TIMESTAMP '{{2. Start date}}'
AND t.day <= TIMESTAMP '{{3. End date}}'
ORDER BY 1