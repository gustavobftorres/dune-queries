-- part of a query repo
-- query name: Balancer V3 LBP Indirect Volume
-- query link: https://dune.com/queries/4838241


WITH lbp_info AS (
        SELECT *
        FROM  query_4837895
        WHERE pool_symbol = '{{LBP}}'
        AND blockchain = '{{blockchain}}'
    ),
    
    lbp_volume as (
        SELECT 
            date_trunc('day', a.block_time) as block_time, 
            SUM(a.amount_usd) as direct_spent
        FROM dex.trades a
        INNER JOIN lbp_info l ON a.token_bought_address = l.project_token 
        OR a.token_sold_address = l.project_token 
        AND l.pool_address = a.project_contract_address
        AND a.project = 'balancer'
        AND a.blockchain = l.blockchain
        WHERE a.block_time BETWEEN l.start_time AND l.end_time
        GROUP BY 1
    ), 

    token_purchases AS (
        SELECT 
            date_trunc('day', a.block_time) as block_time, 
            sum(a.amount_usd) as direct_spent, 
            sum(b.amount_usd) as indirect_spent 
        FROM dex.trades a
        INNER JOIN lbp_info l ON l.pool_address = a.project_contract_address
        INNER JOIN dex.trades b ON a.project_contract_address = l.pool_address
        AND a.token_sold_address = l.project_token
        AND b.token_sold_address <> l.project_token
        AND a.blockchain = l.blockchain
        WHERE a.tx_hash = b.tx_hash
        AND a.project = 'balancer'
        AND b.project = 'balancer'
        AND a.block_time BETWEEN l.start_time AND l.end_time
        group by 1
    ), 

    token_sales AS (
        SELECT 
            date_trunc('day', a.block_time) as block_time, 
            sum(a.amount_usd) as direct_spent, 
            sum(b.amount_usd) as indirect_spent 
        FROM dex.trades a
        INNER JOIN lbp_info l ON l.pool_address = a.project_contract_address
        INNER JOIN dex.trades b ON l.pool_address = a.project_contract_address
        AND a.token_bought_address = l.project_token
        AND b.token_bought_address <> l.project_token
        AND a.blockchain = l.blockchain
        WHERE a.tx_hash = b.tx_hash
        AND a.project = 'balancer'
        AND b.project = 'balancer'
        AND a.block_time BETWEEN l.start_time AND l.end_time
        GROUP BY 1
    )

SELECT 
    COALESCE(z.block_time, COALESCE(p.block_time, s.block_time))  as block_time,
    COALESCE(z.direct_spent,0) lbp_volume, 
    COALESCE(p.indirect_spent,0) + COALESCE(s.indirect_spent,0) as side_volume
FROM token_purchases p
FULL OUTER JOIN token_sales s
ON p.block_time = s.block_time
FULL OUTER JOIN lbp_volume z
ON p.block_time = z.block_time
