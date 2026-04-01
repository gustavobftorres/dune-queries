-- part of a query repo
-- query name: Built on Balancer TVL and Liquidity Utillization
-- query link: https://dune.com/queries/3150793


WITH pools AS (
    SELECT * FROM query_3144841
    WHERE project = '{{Project}}'
    )
    , tvl AS (
        SELECT 
            CAST (day as TIMESTAMP) as day
            , l.pool_id
            , BYTEARRAY_SUBSTRING(l.pool_id, 1, 20) AS pool_address
            , l.blockchain
            , SUM(l.pool_liquidity_usd) AS tvl
        FROM balancer.liquidity l
        WHERE l.pool_liquidity_usd > 1 
            AND l.day >= (CURRENT_DATE - interval '90' day)
        GROUP BY 1,2,3,4
       
       UNION ALL 
        
        SELECT 
            CAST (day as TIMESTAMP) as day
            , l.pool_id
            , BYTEARRAY_SUBSTRING(l.pool_id, 1, 20) AS pool_address
            , l.blockchain
            , SUM(l.protocol_liquidity_usd) AS tvl
        FROM beethoven_x_fantom.liquidity l
        WHERE l.protocol_liquidity_usd > 1 
            AND l.day >= (CURRENT_DATE - interval '90' day)
        GROUP BY 1,2,3,4    
        
       UNION ALL 
        
        SELECT 
            CAST (day as TIMESTAMP) as day
            , l.pool_id
            , BYTEARRAY_SUBSTRING(l.pool_id, 1, 20) AS pool_address
            , l.blockchain
            , SUM(l.protocol_liquidity_usd) AS tvl
        FROM jelly_swap_sei.liquidity l
        WHERE l.protocol_liquidity_usd > 1 
            AND l.day >= (CURRENT_DATE - interval '90' day)
        GROUP BY 1,2,3,4            
        )
        
     , balancer_tvl AS (
        SELECT 
            SUM(l.pool_liquidity_usd) AS tvl
        FROM balancer.liquidity l
        WHERE 
         l.day = (CURRENT_DATE - interval '1' day)
    )   
    , swaps AS (
        SELECT 
            block_date as day
            , t.project_contract_address
            , t.blockchain
            , SUM(t.amount_usd) as volume
        FROM balancer.trades t
            WHERE block_date >= TIMESTAMP '{{Start Date}}'
        GROUP BY 1,2,3
        
       UNION ALL
       
        SELECT 
            block_date as day
            , t.project_contract_address
            , t.blockchain
            , SUM(t.amount_usd) as volume
        FROM beethoven_x.trades t
            WHERE block_date >= TIMESTAMP '{{Start Date}}'
        GROUP BY 1,2,3    
        
       UNION ALL
       
        SELECT 
            block_date as day
            , t.project_contract_address
            , t.blockchain
            , SUM(t.amount_usd) as volume
        FROM jelly_swap.trades t
            WHERE block_date >= TIMESTAMP '{{Start Date}}'
        GROUP BY 1,2,3         
        )
        
    , revenue AS(
        SELECT 
            day
            , f.pool_address
            , f.blockchain
            , SUM(f.protocol_fee_collected_usd) as revenue
        FROM balancer.protocol_fee f
            WHERE day >= TIMESTAMP '{{Start Date}}'
        GROUP BY 1,2,3
        
        UNION ALL
        
        SELECT 
            day
            , f.pool_address
            , f.blockchain
            , SUM(f.protocol_fee_collected_usd) as revenue
        FROM beethoven_x_fantom.protocol_fee f
            WHERE day >= TIMESTAMP '{{Start Date}}'
        GROUP BY 1,2,3
        
        UNION ALL
        
        SELECT 
            day
            , f.pool_address
            , f.blockchain
            , SUM(f.protocol_fee_collected_usd) as revenue
        FROM jelly_swap_sei.protocol_fee f
            WHERE day >= TIMESTAMP '{{Start Date}}'
        GROUP BY 1,2,3)

SELECT 
    w.project
    , t.day
    , SUM(t.tvl)/1e6 AS tvl
    , SUM(t.tvl)*100/(SELECT tvl FROM balancer_tvl) AS perc_tvl
    , SUM(s.volume) AS volume
    , SUM(s.volume)/SUM(t.tvl) AS liq_util
    , SUM(f.revenue) AS revenue
FROM pools w
LEFT JOIN tvl t 
    ON t.pool_id = w.pool_id 
    AND t.blockchain = w.blockchain 
LEFT JOIN swaps s 
    ON s.project_contract_address = BYTEARRAY_SUBSTRING(w.pool_id,1,20) 
    AND s.blockchain = w.blockchain
    AND s.day = t.day
LEFT JOIN revenue f 
    ON f.pool_address = BYTEARRAY_SUBSTRING(w.pool_id,1,20) 
    AND f.blockchain = w.blockchain
    AND f.day = t.day    
WHERE t.day IS NOT NULL
GROUP BY 1, 2
ORDER BY 2 DESC, 3 DESC