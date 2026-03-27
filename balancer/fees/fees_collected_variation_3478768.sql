-- part of a query repo
-- query name: Fees Collected Variation
-- query link: https://dune.com/queries/3478768


WITH 
fees AS (
    SELECT 
    CASE WHEN 
        '{{Aggregation}}' = 'Monthly'
    THEN CAST(DATE_TRUNC('month', t.day) AS TIMESTAMP) 
    WHEN 
        '{{Aggregation}}' = 'Weekly'
    THEN CAST(DATE_TRUNC('week', t.day) AS TIMESTAMP) 
        WHEN 
        '{{Aggregation}}' = 'Daily'
    THEN CAST(DATE_TRUNC('day', t.day) AS TIMESTAMP) 
    END AS date, 
        SUM(protocol_fee_collected_usd) AS amount_usd
    FROM balancer.protocol_fee t
    LEFT JOIN dune.balancer.dataset_core_pools c 
    ON c.network = t.blockchain AND c.pool = t.pool_id
    WHERE t.day >= TIMESTAMP '{{Start Date}}'
    AND ('{{Blockchain}}' = 'All' OR t.blockchain = '{{Blockchain}}')
    AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
    GROUP BY 1
)

SELECT 
    CAST(date AS TIMESTAMP) AS date,
    amount_usd,
    (amount_usd - LAG(amount_usd) OVER (ORDER BY date)) / LAG(amount_usd) OVER (ORDER BY date) AS delta_amount_usd
FROM fees