-- part of a query repo
-- query name: Balancer V2 LBPs Total Raised
-- query link: https://dune.com/queries/226267


WITH lbps AS (
        -- V2 LBPs
        SELECT pool_id, token_sold, end_time, blockchain
        FROM query_2511450
    ),
    lbp_token_out AS (
        SELECT  
            SUM(CAST(token_sold_amount_raw as double)/POWER(10,COALESCE(decimals, 18))) AS amount_out,
            SUM(amount_usd) AS usd_amount_out
        FROM dex.trades d
        INNER JOIN lbps l ON SUBSTRING(l.pool_id,1,42) = CAST(d.project_contract_address as varchar) 
        AND l.token_sold = d.token_sold_address
        AND l.blockchain = d.blockchain
        LEFT JOIN tokens.erc20 t ON t.contract_address = l.token_sold
        WHERE project = 'balancer'
    ),
    
    lbp_token_in AS (
        SELECT  
            SUM(CAST(token_bought_amount_raw as double)/POWER(10,COALESCE(decimals, 18))) AS amount_in,
            SUM(amount_usd) AS usd_amount_in
        FROM dex.trades d
        INNER JOIN lbps l ON SUBSTRING(l.pool_id,1,42) = CAST(d.project_contract_address as varchar) 
        AND l.token_sold = d.token_bought_address
        AND l.blockchain = d.blockchain
        LEFT JOIN tokens.erc20 t ON t.contract_address = l.token_sold
        WHERE project = 'balancer'
    )
    
SELECT SUM(usd_amount_in - usd_amount_out) AS funds_raised
FROM lbp_token_in
JOIN lbp_token_out 
ON 1=1