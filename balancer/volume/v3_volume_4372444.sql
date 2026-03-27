-- part of a query repo
-- query name: V3 Volume
-- query link: https://dune.com/queries/4372444


SELECT 
    SUM(CASE WHEN block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '1' DAY 
        THEN amount_usd END)/1e6 AS volume_24h,
    SUM(CASE WHEN block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '7' DAY 
        THEN amount_usd END)/1e6 AS volume_7d,
    SUM(CASE WHEN block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '30' DAY 
        THEN amount_usd END)/1e6 AS volume_30d,
    SUM(amount_usd)/1e9 AS volume_all_time
FROM balancer.trades 
WHERE ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
AND (version = '3')
