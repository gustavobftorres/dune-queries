-- part of a query repo
-- query name: Project Stats
-- query link: https://dune.com/queries/3148250


WITH pools AS (
    SELECT * FROM query_3144841
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
        GROUP BY 1,2,3,4)
    
    , swaps AS (
        SELECT 
            block_date as day
            , t.project_contract_address
            , t.blockchain
            , SUM(t.amount_usd) as volume
        FROM balancer.trades t
            WHERE block_date >= (CURRENT_DATE - interval '90' day)
        GROUP BY 1,2,3
        
        UNION ALL
        
        SELECT 
            block_date as day
            , t.project_contract_address
            , t.blockchain
            , SUM(t.amount_usd) as volume
        FROM beethoven_x.trades t
            WHERE block_date >= (CURRENT_DATE - interval '90' day)
        GROUP BY 1,2,3
        
        UNION ALL
        
        SELECT 
            block_date as day
            , t.project_contract_address
            , t.blockchain
            , SUM(t.amount_usd) as volume
        FROM jelly_swap.trades t
            WHERE block_date >= (CURRENT_DATE - interval '90' day)
        GROUP BY 1,2,3        
    )

SELECT 
    w.project
    , t.day
    , SUM(t.tvl) AS tvl
    , SUM(s.volume) AS volume
    , SUM(s.volume)/SUM(t.tvl) AS liq_util
FROM pools w
LEFT JOIN tvl t 
    ON t.pool_id = w.poolId 
    AND t.blockchain = w.blockchain 
LEFT JOIN swaps s 
    ON s.project_contract_address = BYTEARRAY_SUBSTRING(w.poolId,1,20) 
    AND s.blockchain = w.blockchain
    AND s.day = t.day
GROUP BY 1, 2
ORDER BY 3 DESC