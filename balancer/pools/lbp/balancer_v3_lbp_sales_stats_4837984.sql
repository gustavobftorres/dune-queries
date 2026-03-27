-- part of a query repo
-- query name: Balancer V3 LBP Sales Stats
-- query link: https://dune.com/queries/4837984


WITH lbp_info AS (
        SELECT *
        FROM  query_4837895
        WHERE pool_symbol = '{{LBP}}'
        AND blockchain = '{{blockchain}}'
    ),
    
    lbp_token_out AS (
        SELECT  
            COALESCE(SUM(CAST(token_bought_amount_raw as double)/power(10,COALESCE(decimals, 18))), 0) AS amount_out,
            COALESCE(SUM(amount_usd), 0) AS usd_amount_out
        FROM dex.trades d
        INNER JOIN lbp_info l
        ON l.pool_address = project_contract_address
        AND l.project_token = d.token_bought_address AND l.blockchain = d.blockchain
        LEFT JOIN tokens.erc20 t
        ON t.contract_address = l.project_token
        WHERE project = 'balancer'
        AND d.block_time <= l.end_time AND t.blockchain = d.blockchain
    ),
    
    lbp_token_in AS (
        SELECT  
            COALESCE(SUM(CAST(token_sold_amount_raw as double)/power(10,COALESCE(decimals, 18))), 0) AS amount_in,
            COALESCE(SUM(amount_usd), 0) AS usd_amount_in
        FROM dex.trades d
        INNER JOIN lbp_info l
        ON l.pool_address = project_contract_address
        AND l.project_token = d.token_sold_address AND l.blockchain = d.blockchain
        LEFT JOIN tokens.erc20  t
        ON t.contract_address = l.project_token
        WHERE project = 'balancer'
        AND d.block_time <= l.end_time AND t.blockchain = d.blockchain
    )
    
SELECT (amount_out - amount_in) AS token_amount, (usd_amount_out - usd_amount_in) AS usd_amount
FROM lbp_token_in
JOIN lbp_token_out 
ON 1=1