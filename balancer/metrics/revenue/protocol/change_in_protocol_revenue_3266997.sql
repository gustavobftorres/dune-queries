-- part of a query repo
-- query name: Change in Protocol Revenue
-- query link: https://dune.com/queries/3266997


WITH weekly_fees as (
SELECT
CASE WHEN '{{Aggregation}}' = 'monthly'
THEN DATE_TRUNC('month', day) 
WHEN '{{Aggregation}}' = 'weekly'
THEN DATE_TRUNC('week', day)
WHEN '{{Aggregation}}' = 'daily'
THEN DATE_TRUNC('day', day)
END AS _date, 
SUM(protocol_fee_collected_usd) AS weekly_protocol_fee     
FROM balancer.protocol_fee     
WHERE day > TIMESTAMP '{{Start Date}}'   
GROUP BY 1), 

_date_over__date AS (
SELECT CAST(_date as TIMESTAMP) as _date,
weekly_protocol_fee, 
weekly_protocol_fee - LAG(weekly_protocol_fee, 1) OVER (ORDER BY _date) AS _date_over__date_change,
(weekly_protocol_fee - LAG(weekly_protocol_fee, 1) OVER (ORDER BY _date)) / LAG(weekly_protocol_fee, 1) OVER (ORDER BY _date) AS _date_over__date_change_perc
FROM weekly_fees) 

SELECT * FROM _date_over__date