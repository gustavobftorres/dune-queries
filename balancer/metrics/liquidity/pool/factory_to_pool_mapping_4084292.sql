-- part of a query repo
-- query name: Factory to Pool Mapping
-- query link: https://dune.com/queries/4084292


SELECT
    q.blockchain,
    q.pool_address,
    q.pool_symbol,
    q.factory_version,
    SUM(pool_liquidity_usd) AS liquidity
FROM query_4080393 q
JOIN balancer.liquidity l ON q.blockchain = l.blockchain
AND q.pool_address = l.pool_address
AND l.day = CURRENT_DATE
WHERE 1 = 1
AND ('{{3. Blockchain}}' = 'All' OR l.blockchain = '{{3. Blockchain}}')
AND ('{{2. Pool Factory}}' = 'All' OR q.factory_version = '{{2. Pool Factory}}')   
AND q.factory_version IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC