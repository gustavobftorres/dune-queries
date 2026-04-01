-- part of a query repo
-- query name: Balancer V2 Pool Token Price
-- query link: https://dune.com/queries/151225


WITH lbp_info AS (
        SELECT *
        FROM dune_user_generated.balancer_v2_lbps
        WHERE name = '{{LBP}}'
    ),
    
    pool_token_price AS (
        SELECT  
            date_trunc('hour', block_time) AS hour,
            l.token_symbol,
            AVG(usd_amount/(token_a_amount_raw/10^COALESCE(decimals, 18))) AS price
        FROM dex.trades d
        INNER JOIN lbp_info l
        ON l.pool_id = d.exchange_contract_address
        AND l.token_sold = d.token_a_address
        LEFT JOIN erc20.tokens t
        ON t.contract_address = l.pool_id
        WHERE project = 'Balancer'
        AND block_time <= l.end_time
        GROUP BY 1, 2
    )

SELECT * FROM pool_token_price