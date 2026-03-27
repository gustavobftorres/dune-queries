-- part of a query repo
-- query name: Core and Non-Core Pools Volume
-- query link: https://dune.com/queries/4226745


SELECT
    DATE_TRUNC('week', block_date) AS week,
    CASE WHEN c.symbol IS NOT NULL
    THEN 'Core'
    ELSE 'Non-Core'
    END AS category,
    SUM(amount_usd) AS volume
FROM balancer.trades t
LEFT JOIN dune.balancer.dataset_core_pools c ON t.blockchain = c.network 
AND t.pool_id = c.pool
WHERE 1 = 1
AND block_date >= TIMESTAMP '2024-01-01'
AND block_date < TIMESTAMP '2025-01-01'
GROUP BY 1, 2