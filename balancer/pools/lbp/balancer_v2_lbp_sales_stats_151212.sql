-- part of a query repo
-- query name: Balancer V2 LBP Sales Stats
-- query link: https://dune.com/queries/151212


WITH lbp_info AS (
        SELECT *
        FROM dune_user_generated.balancer_v2_lbps
        WHERE name = '{{LBP}}'
    ),
    
    lbp_token_out AS (
        SELECT  
            COALESCE(SUM(token_a_amount_raw/10^COALESCE(decimals, 18)), 0) AS amount_out,
            COALESCE(SUM(usd_amount), 0) AS usd_amount_out
        FROM dex.trades d
        INNER JOIN lbp_info l
        ON l.pool_id = d.exchange_contract_address
        AND l.token_sold = d.token_a_address
        LEFT JOIN erc20.tokens t
        ON t.contract_address = l.token_sold
        WHERE project = 'Balancer'
        AND d.block_time <= l.end_time
    ),
    
    lbp_token_in AS (
        SELECT  
            COALESCE(SUM(token_b_amount_raw/10^COALESCE(decimals, 18)), 0) AS amount_in,
            COALESCE(SUM(usd_amount), 0) AS usd_amount_in
        FROM dex.trades d
        INNER JOIN lbp_info l
        ON l.pool_id = d.exchange_contract_address
        AND l.token_sold = d.token_b_address
        LEFT JOIN erc20.tokens t
        ON t.contract_address = l.token_sold
        WHERE project = 'Balancer'
        AND d.block_time <= l.end_time
    )
    
SELECT (amount_out - amount_in) AS token_amount, (usd_amount_out - usd_amount_in) AS usd_amount
FROM lbp_token_in
JOIN lbp_token_out 
ON 1=1