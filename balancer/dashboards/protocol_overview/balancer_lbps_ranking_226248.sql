-- part of a query repo
-- query name: Balancer LBPs Ranking
-- query link: https://dune.com/queries/226248


WITH lbps AS (
        -- V2 LBPs
        SELECT pool_id, name, token_sold, end_time, blockchain
        FROM query_2511450
    ),
    
    lbp_token_out AS (
        SELECT  
            pool_id,
            name,
            SUM(CAST(token_sold_amount_raw as double)/POWER(10,COALESCE(decimals, 18))) AS amount_out,
            SUM(amount_usd) AS usd_amount_out
        FROM dex.trades d
        INNER JOIN lbps l ON SUBSTRING(l.pool_id,1,42) = CAST(d.project_contract_address as varchar) 
        AND l.token_sold = d.token_sold_address
        AND l.blockchain = d.blockchain
        LEFT JOIN tokens.erc20 t ON t.contract_address = l.token_sold
        WHERE project = 'balancer'AND d.block_time <= l.end_time
        GROUP BY 1, 2
    ),
    
    lbp_token_in AS (
        SELECT  
            pool_id, 
            SUM(CAST(token_bought_amount_raw as double)/POWER(10,COALESCE(decimals, 18))) AS amount_out,
            SUM(amount_usd) AS usd_amount_in
        FROM dex.trades d
        INNER JOIN lbps l ON SUBSTRING(l.pool_id,1,42) = CAST(d.project_contract_address as varchar) 
        AND l.token_sold = d.token_sold_address
        AND l.blockchain = d.blockchain
        LEFT JOIN tokens.erc20 t ON t.contract_address = l.token_sold
        WHERE project = 'balancer' AND d.block_time <= l.end_time
        GROUP BY 1
    ),
    
    lbp_volume AS (
        SELECT  
            pool_id, 
            l.blockchain,
            SUM(amount_usd) AS volume
        FROM dex.trades d
        INNER JOIN lbps l
        ON l.pool_id = CAST(d.project_contract_address as varchar) AND l.blockchain = d.blockchain
        WHERE project = 'balancer'
        GROUP BY 1,2
    )
    
SELECT 
    name,
    blockchain || 
        CASE 
            WHEN blockchain = 'arbitrum' THEN ' 🟦'
            WHEN blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN blockchain = 'base' THEN ' 🟨'
            WHEN blockchain = 'ethereum' THEN ' Ξ'
            WHEN blockchain = 'gnosis' THEN ' 🟩'
            WHEN blockchain = 'optimism' THEN ' 🔴'
            WHEN blockchain = 'polygon' THEN ' 🟪'
        END 
    AS blockchain,
    usd_amount_in - usd_amount_out AS funds_raised,
    CONCAT('<a href="https://dune.com/balancer/balancer-v2-lbps?LBP_t8843e=', name, '">view stats</a>') AS stats,
    volume
FROM lbp_volume v
JOIN lbp_token_in i
ON v.pool_id = i.pool_id
JOIN lbp_token_out o 
ON o.pool_id = i.pool_id
ORDER BY 5 DESC