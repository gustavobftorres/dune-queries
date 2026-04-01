-- part of a query repo
-- query name: Balancer New/Old Traders (Dune SQL)
-- query link: https://dune.com/queries/2829184


SELECT
    ssq.time, 
    new_users as "New",
    (unique_users - new_users) as "Old"
FROM (
    SELECT
        sq.time, 
        COUNT(*) AS new_users
    FROM (
        SELECT 
            tx_from as unique_users,
            CASE WHEN '{{1. Aggregation}}' = 'Daily' THEN MIN(date_trunc('day', block_time))
            WHEN '{{1. Aggregation}}' = 'Weekly' THEN MIN(date_trunc('week', block_time))
            WHEN '{{1. Aggregation}}' = 'Monthly' THEN MIN(date_trunc('month', block_time))
            end as "time"
        FROM balancer.trades
        WHERE ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}' 
        AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
        GROUP BY 1
        ORDER BY 1
    ) sq
    GROUP BY 1
) ssq
LEFT JOIN (
        SELECT 
            CASE WHEN '{{1. Aggregation}}' = 'Daily' THEN date_trunc('day', block_time)
            WHEN '{{1. Aggregation}}' = 'Weekly' THEN date_trunc('week', block_time)
            WHEN '{{1. Aggregation}}' = 'Monthly' THEN date_trunc('month', block_time)
            end as "time",
            COUNT(DISTINCT tx_from) AS unique_users
        FROM balancer.trades
        WHERE ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
        AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
        GROUP BY 1
        ORDER BY 1
) t2 ON t2.time = ssq.time
ORDER BY 1 DESC
