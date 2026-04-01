-- part of a query repo
-- query name: Balancer V2 LBP, Hourly (Dune SQL)
-- query link: https://dune.com/queries/2836798


WITH lbp_info AS (
    SELECT *
    FROM query_2511450
    WHERE name = '{{LBP}}'
)

SELECT 
    date_trunc('hour', block_time) AS hour, 
    CONCAT(l.token_symbol, ' LBP') AS symbol,
    COUNT(*) AS transactions,
    SUM(amount_usd) AS volume,
    SUM(CASE WHEN token_bought_address = l.token_sold THEN 1 ELSE 0 END) AS sales,
    SUM(CASE WHEN token_bought_address != l.token_sold THEN 1 ELSE 0 END) AS purchases
FROM balancer.trades t
INNER JOIN lbp_info l ON l.pool_id = CAST(t.project_contract_address as varchar)
WHERE block_time <= l.end_time AND block_time >= l.start_time AND t.blockchain = l.blockchain
GROUP BY 1, 2