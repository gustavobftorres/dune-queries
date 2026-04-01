-- part of a query repo
-- query name: BAL Minted and Fees Collected by Gauge - no parameters
-- query link: https://dune.com/queries/3480969


WITH 
bal_price AS(
SELECT 
    DATE_TRUNC('day', minute) AS day,
    APPROX_PERCENTILE(price, 0.5) AS price
FROM prices.usd
WHERE symbol = 'BAL' AND blockchain = 'ethereum'
GROUP BY 1
),

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

daily_bal_emissions_and_fees AS(
SELECT 
    b.day,
    b.gauge,
    m.pool_address,
    m.blockchain,
    b.symbol,
    emissions AS daily_emissions,
    emissions * price AS daily_emissions_usd,
    SUM(protocol_fee_collected_usd) AS daily_fees
FROM daily_bal_emissions b
LEFT JOIN labels.balancer_gauges m ON b.gauge = m.address
LEFT JOIN balancer.protocol_fee f ON 
f.pool_address = m.pool_address AND 
f.blockchain = m.blockchain AND 
f.day = b.day
LEFT JOIN bal_price p ON p.day = b.day
WHERE symbol IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7)

SELECT 
    day,
    blockchain,
    gauge,
    pool_address,
    symbol,
    SUM(daily_emissions) AS monthly_emissions,
    SUM(daily_emissions_usd) AS monthly_emissions_usd,
    SUM(daily_fees) AS monthly_fees
FROM daily_bal_emissions_and_fees
WHERE day < CURRENT_DATE
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC, 6 DESC, 7 DESC