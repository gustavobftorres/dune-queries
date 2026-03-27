-- part of a query repo
-- query name: BAL emissions x OP Incentives Analysis
-- query link: https://dune.com/queries/3801590


WITH 
opt_price AS(
SELECT 
    DATE_TRUNC('week', minute) AS week,
    APPROX_PERCENTILE(price, 0.5) AS price
FROM prices.usd
WHERE symbol = 'OP' AND blockchain = 'optimism'
GROUP BY 1
),

opt_incentives AS(
SELECT
    q.week,
    SUM(token_amount) AS weekly_incentives_opt,
    SUM(token_amount * price) AS weekly_incentives_opt_usd
FROM query_3796560 q
LEFT JOIN opt_price a ON q.week = a.week
GROUP BY 1
),

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
WHERE time >= TIMESTAMP '2023-11-06'
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
    round_id,
    start_date,
    end_date,
    gauge,
    symbol,
    pct_votes
FROM query_756468
LEFT JOIN days ON DATE_TRUNC('week', day) = DATE_TRUNC('week', CAST(start_date AS TIMESTAMP))
),

daily_bal_emissions_2 AS(
SELECT 
    b.day,
    start_date,
    end_date,
    round_id,
    gauge,
    symbol,
    day_rate * pct_votes AS emissions
FROM bal_supply b
LEFT JOIN gauge_votes v on v.day = b.day
WHERE symbol IS NOT NULL
),

weekly_bal_emissions AS(
SELECT 
    DATE_TRUNC('week', b.day) AS week,
    sum(emissions) AS weekly_emissions,
    sum(emissions * price) AS weekly_emissions_usd
FROM daily_bal_emissions_2 b
INNER JOIN dune.balancer.result_gauge_to_pool_mapping m 
ON m.gauge_address = b.gauge
AND m.blockchain = 'optitrum'
LEFT JOIN bal_price p ON p.day = b.day
GROUP BY 1)

SELECT 
    d.week,
    SUM(weekly_emissions) AS weekly_emissions_bal,
    SUM(weekly_emissions_usd) AS weekly_emissions_bal_usd,
    SUM(weekly_incentives_opt) AS weekly_incentives_opt,
    SUM(weekly_incentives_opt_usd) AS weekly_incentives_opt_usd,
    SUM(weekly_emissions_usd) / SUM(weekly_incentives_opt_usd) AS bal_to_opt_ratio
FROM weekly_bal_emissions d
INNER JOIN opt_incentives a ON d.week = a.week
WHERE d.week <= TIMESTAMP '{{End date}}'
AND d.week >= TIMESTAMP '{{Start date}}'
GROUP BY 1

UNION

SELECT
NULL,
NULL,
NULL,
NULL,
NULL,
NULL

ORDER BY 1 DESC

