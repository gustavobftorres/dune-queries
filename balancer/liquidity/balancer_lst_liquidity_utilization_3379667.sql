-- part of a query repo
-- query name: Balancer LST Liquidity Utilization
-- query link: https://dune.com/queries/3379667


WITH 
    swaps AS (
        SELECT
    CASE WHEN 
        '{{5. Aggregation}}' = 'Monthly'
    THEN CAST(block_month AS TIMESTAMP) 
    WHEN 
        '{{5. Aggregation}}' = 'Weekly'
    THEN CAST(DATE_TRUNC('week', block_date) AS TIMESTAMP) 
        WHEN 
        '{{5. Aggregation}}' = 'Daily'
    THEN CAST(DATE_TRUNC('day', block_date) AS TIMESTAMP) 
    END AS date,
            SUM(amount_usd) AS volume
        FROM balancer.trades d
        INNER JOIN dune.balancer.result_lst_pools l 
            ON l.pool_address = d.project_contract_address
                        AND l.blockchain = d.blockchain
        WHERE
         ('{{3. Blockchain}}' = 'All' OR d.blockchain = '{{3. Blockchain}}')
        GROUP BY 1
    ),

    total_tvl AS (
        SELECT 
    CASE WHEN 
        '{{5. Aggregation}}' = 'Monthly'
    THEN CAST(DATE_TRUNC('month', day) AS TIMESTAMP) 
    WHEN 
        '{{5. Aggregation}}' = 'Weekly'
    THEN CAST(DATE_TRUNC('week', day) AS TIMESTAMP) 
        WHEN 
        '{{5. Aggregation}}' = 'Daily'
    THEN CAST(DATE_TRUNC('day', day) AS TIMESTAMP) 
    END AS date, 
            SUM(protocol_liquidity_usd) AS tvl
        FROM balancer.liquidity l
            INNER JOIN dune.balancer.result_lst_pools p
                ON p.pool_address = l.pool_address
        WHERE ('{{3. Blockchain}}' = 'All' OR l.blockchain = '{{3. Blockchain}}')
        AND protocol_liquidity_usd < 1000000000
        GROUP BY 1
    )
   
SELECT
    CAST(t.date as timestamp) as date,
    (s.volume)/(t.tvl) AS Ratio,
    s.volume,
    t.tvl
FROM total_tvl t
LEFT JOIN swaps s ON s.date = t.date
WHERE t.date >= TIMESTAMP '{{1. Start date}}'
AND t.date <= TIMESTAMP '{{2. End date}}'
ORDER BY 1
