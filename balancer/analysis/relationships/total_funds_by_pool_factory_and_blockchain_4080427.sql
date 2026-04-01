-- part of a query repo
-- query name: Total Funds by pool factory and blockchain
-- query link: https://dune.com/queries/4080427


SELECT
    f.blockchain,
    f.factory_address,
    f.factory_version,
    SUM(pool_liquidity_usd) AS total_funds
FROM query_4080393 f
LEFT JOIN balancer.liquidity l ON f.blockchain = l.blockchain
AND f.pool_address = l.pool_address 
AND l.day = (SELECT max(day) FROM balancer.liquidity)
WHERE f.factory_version IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 4 DESC