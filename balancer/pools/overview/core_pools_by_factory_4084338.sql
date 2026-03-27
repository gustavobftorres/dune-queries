-- part of a query repo
-- query name: Core Pools by Factory
-- query link: https://dune.com/queries/4084338


SELECT
    q.factory_version,
    SUM(CASE WHEN c.symbol IS NOT NULL THEN 1 ELSE 0 END) AS core_pools_count
FROM query_4080393 q
JOIN dune.balancer.dataset_core_pools c ON q.blockchain = c.network
AND BYTEARRAY_SUBSTRING(c.pool, 1, 20) = q.pool_address
WHERE 1 = 1
AND ('{{3. Blockchain}}' = 'All' OR q.blockchain = '{{3. Blockchain}}')
AND ('{{2. Pool Factory}}' = 'All' OR q.factory_version = '{{2. Pool Factory}}')   
AND q.factory_version IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC