-- part of a query repo
-- query name: BAL Minted by Gauge (no params)
-- query link: https://dune.com/queries/3456471


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
)

SELECT 
    b.day,
    gauge,
    symbol,
    day_rate * pct_votes AS emissions
FROM bal_supply b
LEFT JOIN gauge_votes v on v.day = b.day
WHERE symbol IS NOT NULL
ORDER BY 1 DESC, 4 DESC