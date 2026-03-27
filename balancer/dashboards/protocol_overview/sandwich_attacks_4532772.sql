-- part of a query repo
-- query name: Sandwich Attacks
-- query link: https://dune.com/queries/4532772


SELECT
    t1.block_date,
    t1.version,
    COUNT(t1.tx_hash) AS total_flow,
    COUNT(t2.tx_hash) AS toxic_flow,
    CASE 
        WHEN CAST(COUNT(t1.tx_hash) AS DOUBLE) = 0 THEN 0
        ELSE CAST(COUNT(t2.tx_hash) AS DOUBLE) / CAST(COUNT(t1.tx_hash) AS DOUBLE)
    END AS toxic_flow_pct,
        CASE 
        WHEN CAST(SUM(t1.amount_usd) AS DOUBLE) = 0 THEN 0
        ELSE CAST(SUM(t2.amount_usd) AS DOUBLE) / CAST(SUM(t1.amount_usd) AS DOUBLE)
    END AS toxic_flow_volume_pct
FROM balancer.trades t1
LEFT JOIN dex.sandwiches t2 
    ON t1.tx_hash = t2.tx_hash
WHERE ('{{version}}' = 'All' OR t1.version = '{{version}}')
  AND ('{{blockchain}}' = 'All' OR t1.blockchain = '{{blockchain}}')
  AND ('{{pool_address}}' = 'All' OR CAST(t1.project_contract_address AS VARCHAR) = '{{pool_address}}')
  AND block_date >= TIMESTAMP '{{start}}'
GROUP BY 1, 2
ORDER BY 1 DESC, 2 DESC;
