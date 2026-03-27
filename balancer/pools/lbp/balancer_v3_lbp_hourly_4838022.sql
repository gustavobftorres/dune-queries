-- part of a query repo
-- query name: Balancer V3 LBP, Hourly
-- query link: https://dune.com/queries/4838022


WITH lbp_info AS (
    SELECT *
    FROM  query_4837895
    WHERE pool_symbol = '{{LBP}}'
    AND blockchain = '{{blockchain}}'
)

SELECT 
    date_trunc('hour', block_time) AS hour, 
    CONCAT(l.project_token_symbol, ' LBP') AS symbol,
    COUNT(*) AS transactions,
    SUM(amount_usd) AS volume,
    SUM(CASE WHEN token_bought_address = l.project_token THEN 1 ELSE 0 END) AS sales,
    SUM(CASE WHEN token_bought_address != l.project_token THEN 1 ELSE 0 END) AS purchases
FROM balancer.trades t
INNER JOIN lbp_info l ON l.pool_address = t.project_contract_address
WHERE block_time <= l.end_time AND block_time >= l.start_time AND t.blockchain = l.blockchain
GROUP BY 1, 2