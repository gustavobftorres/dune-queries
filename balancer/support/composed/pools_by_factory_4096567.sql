-- part of a query repo
-- query name: Pools by Factory
-- query link: https://dune.com/queries/4096567


SELECT
    q.factory_version,
    COUNT(*) AS pool_count
FROM query_4080393 q
WHERE 1 = 1
AND ('{{3. Blockchain}}' = 'All' OR q.blockchain = '{{3. Blockchain}}')
AND ('{{2. Pool Factory}}' = 'All' OR q.factory_version = '{{2. Pool Factory}}')   
AND q.factory_version IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC