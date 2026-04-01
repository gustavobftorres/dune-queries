-- part of a query repo
-- query name: balancer volume by source (2 label sources)
-- query link: https://dune.com/queries/4397295


SELECT DISTINCT 
    LOWER(COALESCE(l.owner_key, q.name)) AS source,
    SUM(t.amount_usd) AS volume
FROM balancer.trades t
LEFT JOIN labels.owner_addresses l ON t.tx_to = l.address
AND t.blockchain = l.blockchain
LEFT JOIN query_3004790 q ON CAST(t.tx_to AS VARCHAR)= q.address
AND t.blockchain = q.blockchain
WHERE t.block_date >= CURRENT_DATE - INTERVAL '365' day
AND COALESCE(l.owner_key, q.name) IS NOT NULL
AND LOWER(COALESCE(l.owner_key, q.name)) NOT IN ('arbitrage bot', 'balancer', 'gnosis_safe')
GROUP BY 1
ORDER BY 2 DESC