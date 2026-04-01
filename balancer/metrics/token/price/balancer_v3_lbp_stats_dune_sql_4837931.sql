-- part of a query repo
-- query name: Balancer V3 LBP Stats (Dune SQL)
-- query link: https://dune.com/queries/4837931


WITH lbp_info AS (
        SELECT *
        FROM  query_4837895
        WHERE pool_symbol = '{{LBP}}'
        AND blockchain = '{{blockchain}}'
    )

 SELECT 
    SUBSTRING(CAST(DATE_TRUNC('day',end_time) - DATE_TRUNC('day',start_time) as varchar),1,1) AS duration,
    COUNT(DISTINCT tx_from) AS participants, 
    (SELECT COUNT(*) FROM dex.trades t
    INNER JOIN lbp_info l ON pool_address = project_contract_address
    WHERE project = 'balancer' ) AS txns,
    (SELECT SUM(amount_usd) FROM dex.trades t
    INNER JOIN lbp_info l ON pool_address = project_contract_address
    WHERE project = 'balancer' 
    AND block_time <= l.end_time) AS volume
FROM dex.trades t
INNER JOIN lbp_info l ON pool_address = project_contract_address
AND l.project_token = t.token_bought_address AND l.blockchain = t.blockchain
WHERE project = 'balancer'
AND block_time <= l.end_time
AND block_time >= l.start_time
GROUP BY 1