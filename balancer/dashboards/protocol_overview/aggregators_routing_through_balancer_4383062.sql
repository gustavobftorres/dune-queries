-- part of a query repo
-- query name: Aggregators routing through balancer
-- query link: https://dune.com/queries/4383062


SELECT DISTINCT 
    a.project,
    SUM(t.amount_usd) AS volume
FROM balancer.trades t
INNER JOIN dex_aggregator.trades a ON t.blockchain = a.blockchain
AND t.tx_hash = a.tx_hash
WHERE t.block_date >= CURRENT_DATE - INTERVAL '365' day
GROUP BY 1
ORDER BY 2 DESC