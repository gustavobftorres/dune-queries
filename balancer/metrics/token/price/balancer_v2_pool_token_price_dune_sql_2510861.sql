-- part of a query repo
-- query name: Balancer V2 Pool Token Price (Dune SQL)
-- query link: https://dune.com/queries/2510861


WITH lbp_info AS (
        SELECT *
        FROM query_2511450
        WHERE name = '{{LBP}}'
    ),
    
    pool_token_price AS (
        SELECT  
            date_trunc('hour', block_time) AS hour,
            l.token_symbol,
            AVG(amount_usd/(CAST(token_sold_amount_raw as double)/power(10,COALESCE(decimals, 18)))) AS price
        FROM dex.trades d
        INNER JOIN lbp_info l
        ON l.pool_id = CAST(d.project_contract_address as varchar)
        AND l.token_sold = d.token_sold_address
        AND d.blockchain = l.blockchain
        LEFT JOIN tokens.erc20 t
        ON CAST(t.contract_address as varchar) = l.pool_id
        AND t.blockchain = d.blockchain
        WHERE project = 'balancer'
        AND block_time <= l.end_time
        GROUP BY 1, 2
    )

SELECT * FROM pool_token_price