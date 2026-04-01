-- part of a query repo
-- query name: Balancer V3 Volume by Pool Type
-- query link: https://dune.com/queries/4373461


/* Volume per week */
/* Visualization: bar chart */

    SELECT
    CASE 
        WHEN '{{aggregation}}' = 'daily' THEN block_date
        WHEN '{{aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
        WHEN '{{aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
    END AS week,
        CONCAT('v', version, ': ', pool_type) AS pool_type,
        SUM(amount_usd) AS volume
    FROM balancer.trades t
    WHERE 1 = 1
    AND ('{{blockchain}}' = 'All' OR t.blockchain = '{{blockchain}}')
    AND (version = '3')
    GROUP BY 1, 2
