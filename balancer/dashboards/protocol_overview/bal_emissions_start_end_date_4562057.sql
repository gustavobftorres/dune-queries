-- part of a query repo
-- query name: BAL Emissions start/end date
-- query link: https://dune.com/queries/4562057


WITH a AS(
SELECT 
    time,
    week_rate,
    day_rate,
    ROW_NUMBER() OVER (PARTITION BY week_rate ORDER BY time ASC) AS rn,
    ROW_NUMBER() OVER (PARTITION BY week_rate ORDER BY time DESC) AS nr
FROM query_3140829)

SELECT
    a.week_rate * 52, 
    a.day_rate,
    a.time AS start_date,
    b.time AS end_date
FROM a
CROSS JOIN a b 
WHERE a.rn = 1
AND b.nr = 1
AND a.week_rate = b.week_rate
