-- part of a query repo
-- query name: Balancer V3 Fees Collected by Pool Type
-- query link: https://dune.com/queries/4373573


    SELECT 
    CASE 
        WHEN '{{aggregation}}' = 'daily' THEN day
        WHEN '{{aggregation}}' = 'weekly' THEN DATE_TRUNC('week', day)
        WHEN '{{aggregation}}' = 'monthly' THEN DATE_TRUNC('month', day)
    END AS date,
        pool_type,
        SUM(protocol_fee_collected_usd) AS fees_usd
    FROM balancer.protocol_fee t
    WHERE 1 = 1
    AND ('{{blockchain}}' = 'All' OR t.blockchain = '{{blockchain}}')
    AND (version = '3')
    GROUP BY 1, 2
