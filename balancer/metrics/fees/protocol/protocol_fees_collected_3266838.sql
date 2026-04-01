-- part of a query repo
-- query name: Protocol Fees Collected
-- query link: https://dune.com/queries/3266838


SELECT 
    CASE WHEN '{{Aggregation}}' = 'Monthly'
    THEN DATE_TRUNC('month', day) 
    WHEN '{{Aggregation}}' = 'Weekly'
    THEN DATE_TRUNC('week', day)
    WHEN '{{Aggregation}}' = 'Daily'
    THEN DATE_TRUNC('day', day)
    END AS _date, 
    SUM(protocol_fee_collected_usd) AS protocol_revenue,
    SUM(treasury_fee_usd) AS treasury_revenue
FROM balancer.protocol_fee
WHERE ('{{Blockchain}}' = 'All' OR blockchain = '{{Blockchain}}')
AND day > TIMESTAMP '{{Start Date}}'
GROUP BY 1