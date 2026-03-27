-- part of a query repo
-- query name: Treasury Revenue
-- query link: https://dune.com/queries/3228122


SELECT 
    CASE WHEN '{{Aggregation}}' = 'monthly'
    THEN DATE_TRUNC('month', day) 
    WHEN '{{Aggregation}}' = 'weekly'
    THEN DATE_TRUNC('week', day)
    WHEN '{{Aggregation}}' = 'daily'
    THEN DATE_TRUNC('day', day)
    END AS _date, 
    SUM(treasury_fee_usd) AS treasury_revenue--,
--    SUM(treasury_fee_usd) OVER (ORDER BY day ASC) AS "All-Time Volume"
FROM balancer.protocol_fee
WHERE ('{{Blockchain}}' = 'All' OR blockchain = '{{Blockchain}}')
AND day > TIMESTAMP '{{Start Date}}'
GROUP BY 1