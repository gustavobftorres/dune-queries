-- part of a query repo
-- query name: Protocol Fees by Pool Type
-- query link: https://dune.com/queries/3544846


SELECT 
    CASE WHEN '{{Aggregation}}' = 'Monthly'
    THEN DATE_TRUNC('month', day) 
    WHEN '{{Aggregation}}' = 'Weekly'
    THEN DATE_TRUNC('week', day)
    WHEN '{{Aggregation}}' = 'Daily'
    THEN DATE_TRUNC('day', day)
    END AS _date,
    pool_type,
SUM(protocol_fee_collected_usd) as protocol,
SUM(treasury_fee_usd) as treasury
FROM balancer.protocol_fee f 
LEFT JOIN dune.balancer.dataset_core_pools c 
ON c.network = f.blockchain AND c.pool = f.pool_id
WHERE day >= TIMESTAMP '{{Start Date}}'
AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
AND ('{{Blockchain}}' = 'All' OR f.blockchain = '{{Blockchain}}')
GROUP BY 1, 2