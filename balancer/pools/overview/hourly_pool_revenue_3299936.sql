-- part of a query repo
-- query name: Hourly Pool Revenue
-- query link: https://dune.com/queries/3299936


WITH pool_data as(
SELECT 
    pool_address,
    blockchain,
    pool_registered
FROM query_2634572
),

time_stamps as(
SELECT 
pool_address,
blockchain,
CASE WHEN pool_registered < TIMESTAMP '{{1. Start Date}}'
THEN TIMESTAMP '{{1. Start Date}}'
ELSE pool_registered
END AS start_time, --calculates start time as a maximum between start date parameter and pool registered time. 
CASE WHEN now() < TIMESTAMP '{{2. End Date}}'
THEN now()
ELSE TIMESTAMP '{{2. End Date}}'
END AS end_time --calculates end time as a minimum between today and the end date parameter
FROM pool_data

)

SELECT 
    f.pool_address, 
    f.pool_symbol, 
    f.blockchain, 
    date_diff('hour', d.start_time, d.end_time) as pool_time,
    sum(f.protocol_fee_collected_usd) as total_revenue,
    sum(f.protocol_fee_collected_usd) / date_diff('hour', d.start_time, d.end_time) as hourly_revenue,
    sum(f.treasury_fee_usd) as treasury_revenue,
    sum(f.treasury_fee_usd) / date_diff('hour', d.start_time, d.end_time) as hourly_treasury_revenue
FROM balancer.protocol_fee f
LEFT JOIN time_stamps d ON d.blockchain = f.blockchain AND d.pool_address = f.pool_address
WHERE f.day > TIMESTAMP '{{1. Start Date}}'
AND f.day <= TIMESTAMP '{{2. End Date}}'
AND '{{3. Blockchain}}' = 'All' OR f.blockchain = '{{3. Blockchain}}'
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC