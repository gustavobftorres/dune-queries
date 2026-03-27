-- part of a query repo
-- query name: Balancer V2 LBP Sales Stats (Dune SQL)
-- query link: https://dune.com/queries/2836761


WITH lbp_info AS (
        SELECT *
        FROM query_2511450
        WHERE name = '{{LBP}}'
    ),
    
    lbp_token_out AS (
        SELECT  
            COALESCE(SUM(CAST(token_bought_amount_raw as double)/power(10,COALESCE(decimals, 18))), 0) AS amount_out,
            COALESCE(SUM(amount_usd), 0) AS usd_amount_out
        FROM dex.trades d
        INNER JOIN lbp_info l
        ON SUBSTRING(CAST(l.pool_id as varchar),1,42) = CAST(d.project_contract_address as varchar)
        AND l.token_sold = d.token_bought_address AND l.blockchain = d.blockchain
        LEFT JOIN tokens.erc20 t
        ON CAST(t.contract_address as varchar) = CAST(l.token_sold as varchar)
        WHERE project = 'balancer'
        AND d.block_time <= l.end_time AND t.blockchain = d.blockchain
    ),
    
    lbp_token_in AS (
        SELECT  
            COALESCE(SUM(CAST(token_sold_amount_raw as double)/power(10,COALESCE(decimals, 18))), 0) AS amount_in,
            COALESCE(SUM(amount_usd), 0) AS usd_amount_in
        FROM dex.trades d
        INNER JOIN lbp_info l
        ON SUBSTRING(CAST(l.pool_id as varchar),1,42) = CAST(d.project_contract_address as varchar)
        AND l.token_sold = d.token_sold_address AND l.blockchain = d.blockchain
        LEFT JOIN tokens.erc20  t
        ON CAST(t.contract_address as varchar) = CAST(l.token_sold as varchar)
        WHERE project = 'balancer'
        AND d.block_time <= l.end_time AND t.blockchain = d.blockchain
    )
    
SELECT (amount_out - amount_in) AS token_amount, (usd_amount_out - usd_amount_in) AS usd_amount
FROM lbp_token_in
JOIN lbp_token_out 
ON 1=1