-- part of a query repo
-- query name: Protocol Fees Collected by Version
-- query link: https://dune.com/queries/4659056


WITH pool_labels AS(
    SELECT
        blockchain,
        address,
        pool_type
    FROM labels.balancer_v2_pools

    UNION

    SELECT
        blockchain,
        address,
        pool_type
    FROM labels.balancer_v3_pools
)

SELECT 
    CASE WHEN '{{Aggregation}}' = 'Monthly'
    THEN DATE_TRUNC('month', day) 
    WHEN '{{Aggregation}}' = 'Weekly'
    THEN DATE_TRUNC('week', day)
    WHEN '{{Aggregation}}' = 'Daily'
    THEN DATE_TRUNC('day', day)
    END AS _date, 
    version,  
    SUM(protocol_fee_collected_usd) as protocol
FROM balancer.protocol_fee f 
LEFT JOIN dune.balancer.dataset_core_pools c 
ON c.network = f.blockchain AND c.pool = f.pool_id
LEFT JOIN pool_labels l 
ON l.blockchain = f.blockchain AND l.address = f.pool_address
WHERE day >= TIMESTAMP '{{Start Date}}'
AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
AND ('{{Pool Type}}' = 'All' OR l.pool_type = '{{Pool Type}}')
GROUP BY 1, 2