-- part of a query repo
-- query name: Balancer V3 Fees Collected
-- query link: https://dune.com/queries/4373539


SELECT
    CASE 
        WHEN '{{aggregation}}' = 'daily' THEN day
        WHEN '{{aggregation}}' = 'weekly' THEN DATE_TRUNC('week', day)
        WHEN '{{aggregation}}' = 'monthly' THEN DATE_TRUNC('month', day)
    END AS date,
    fee_type,
    SUM(protocol_fee_collected_usd) AS fees_usd
FROM balancer.protocol_fee
WHERE version = '3'
AND ('{{blockchain}}' = 'All' or blockchain = '{{blockchain}}')
GROUP BY 1, 2
ORDER BY 1 DESC