-- part of a query repo
-- query name: Balancer V2 LBP Indirect Volume (Dune SQL)
-- query link: https://dune.com/queries/2836807


WITH lbp_info AS (
        SELECT *
        FROM query_2511450
        WHERE name = '{{LBP}}'
    ),
    
    lbp_volume as (
        SELECT 
            date_trunc('day', a.block_time) as block_time, 
            SUM(a.amount_usd) as direct_spent
        FROM dex.trades a
        INNER JOIN lbp_info l ON a.token_bought_address = l.token_sold 
        OR a.token_sold_address = l.token_sold 
        AND l.pool_id = CAST(a.project_contract_address as varchar)
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
        INNER JOIN lbp_info l ON l.pool_id = CAST(a.project_contract_address as varchar)
        INNER JOIN dex.trades b ON CAST(a.project_contract_address as varchar) = l.pool_id
        AND a.token_sold_address = l.token_sold
        AND b.token_sold_address <> l.token_sold
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
        INNER JOIN lbp_info l ON l.pool_id = CAST(a.project_contract_address as varchar)
        INNER JOIN dex.trades b ON l.pool_id= CAST(a.project_contract_address as varchar)
        AND a.token_bought_address = l.token_sold
        AND b.token_bought_address <> l.token_sold
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


-- select max(block_time) from dex.trades