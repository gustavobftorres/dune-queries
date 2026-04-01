-- part of a query repo
-- query name: Balancer Weekly Liquidity Utilization, by Version
-- query link: https://dune.com/queries/2652982


WITH 
    swaps AS (
        SELECT
            date_trunc('week', d.block_time) AS day,
            version,
            SUM(amount_usd) AS volume
        FROM balancer.trades d
        WHERE ('{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}')
        GROUP BY 1, 2
    ),

    total_tvl AS (
        SELECT 
            date_trunc('week', day) AS day, 
            version,
            SUM(protocol_liquidity_usd) AS tvl
        FROM balancer.liquidity
        WHERE ('{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}')
        AND protocol_liquidity_usd < 1000000000
        AND day <= (SELECT MAX(day) FROM balancer.liquidity WHERE version = '2')
        GROUP BY 1, 2
    )
   
SELECT
    CAST(t.day as timestamp) as day,
    s.version,
    (s.volume)/(t.tvl) AS Ratio,
    s.volume,
    t.tvl
FROM total_tvl t
LEFT JOIN swaps s ON s.day = t.day
AND s.version = t.version
WHERE t.day >= TIMESTAMP '{{1. Start date}}'
AND t.day <= TIMESTAMP '{{2. End date}}'
ORDER BY 1
