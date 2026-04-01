-- part of a query repo
-- query name: BAL Minted and Fees Collected by Gauge
-- query link: https://dune.com/queries/3454598


WITH 
bal_supply AS(
SELECT 
    time AS day,
    DATE_TRUNC('week', time) AS week,
    day_rate,
    week_rate
FROM query_2846023
),

days AS 
(
    with days_seq AS (
        SELECT
        sequence(
            (SELECT CAST(min(DATE_TRUNC('day', CAST(start_date AS timestamp))) AS timestamp) day FROM query_756468 tr)
            , DATE_TRUNC('day', CAST(now() AS timestamp))
            , interval '1' day) AS day
    )
    
    SELECT 
        days.day
    FROM days_seq
    CROSS JOIN unnest(day) AS days(day)
),

gauge_votes AS(
SELECT
    day + interval '3' day AS day, --workaround for daily votes
    gauge,
    symbol,
    pct_votes
FROM query_756468
LEFT JOIN days ON DATE_TRUNC('week', day) = DATE_TRUNC('week', CAST(start_date AS TIMESTAMP))
),

daily_bal_emissions AS(
SELECT 
    b.day,
    gauge,
    symbol,
    day_rate * pct_votes AS emissions
FROM bal_supply b
LEFT JOIN gauge_votes v on v.day = b.day
WHERE symbol IS NOT NULL
),

daily_bal_emissions_and_revenue AS(
SELECT 
    b.day,
    b.gauge,
    m.pool_address,
    m.blockchain,
    b.symbol,
    emissions AS daily_emissions,
    SUM(protocol_fee_collected_usd) AS daily_revenue
FROM daily_bal_emissions b
LEFT JOIN dune.balancer.result_gauge_to_pool_mapping m ON m.gauge_address = b.gauge
LEFT JOIN balancer.protocol_fee f ON 
f.pool_address = m.pool_address AND 
f.blockchain = m.blockchain AND 
f.day = b.day
LEFT JOIN dune.balancer.dataset_core_pools c 
ON c.network = f.blockchain AND c.pool = f.pool_id
WHERE b.symbol IS NOT NULL
AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
GROUP BY 1, 2, 3, 4, 5, 6)

SELECT 
    CASE WHEN '{{Aggregation}}' = 'Monthly'
    THEN CAST(DATE_TRUNC('month', day) AS DATE)
    WHEN '{{Aggregation}}' = 'Weekly'
    THEN CAST(DATE_TRUNC('week', day) AS DATE)
    WHEN '{{Aggregation}}' = 'Daily'
    THEN CAST(DATE_TRUNC('day', day) AS DATE)
    END AS _date,
    gauge,
    pool_address,
    symbol,
    SUM(daily_emissions) AS monthly_emissions,
    SUM(daily_revenue) AS monthly_revenue
FROM daily_bal_emissions_and_revenue
WHERE  
day > TIMESTAMP '{{Start Date}}'
AND ('{{Pool Address}}' = 'All' OR CAST(pool_address AS VARCHAR) = '{{Pool Address}}')
AND ('{{Blockchain}}' = 'All' OR blockchain = '{{Blockchain}}')
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 5 DESC, 6 DESC