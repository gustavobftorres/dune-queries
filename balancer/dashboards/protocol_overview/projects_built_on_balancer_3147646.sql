-- part of a query repo
-- query name: Projects Built on Balancer
-- query link: https://dune.com/queries/3147646


WITH pools AS (
    SELECT * FROM query_3144841
    )
    
    , tvl AS (
        SELECT 
            l.pool_id
            , BYTEARRAY_SUBSTRING(l.pool_id, 1, 20) AS pool_address
            , l.blockchain
            , SUM(l.protocol_liquidity_usd) AS tvl
        FROM beethoven_x_fantom.liquidity l
        WHERE l.protocol_liquidity_usd > 1 
            AND l.day = (CURRENT_DATE - interval '1' day)
        GROUP BY 1,2,3
        
        UNION ALL 
        
         SELECT 
            l.pool_id
            , BYTEARRAY_SUBSTRING(l.pool_id, 1, 20) AS pool_address
            , l.blockchain
            , SUM(l.protocol_liquidity_usd) AS tvl
        FROM jelly_swap_sei.liquidity l
        WHERE l.protocol_liquidity_usd > 1 
            AND l.day = (CURRENT_DATE - interval '1' day)
        GROUP BY 1,2,3
        
        UNION ALL        
        
        SELECT DISTINCT
            l.pool_id
            , BYTEARRAY_SUBSTRING(l.pool_id, 1, 20) AS pool_address
            , l.blockchain
            , SUM(l.pool_liquidity_usd) AS tvl
        FROM balancer.liquidity l
        WHERE l.pool_liquidity_usd > 1 
            AND l.day = (CURRENT_DATE - interval '1' day)
        GROUP BY 1,2,3
    )
    
    , swaps AS (
        SELECT 
            t.project_contract_address
            , t.blockchain
            , SUM(t.amount_usd) FILTER(WHERE t.block_time >= now() - INTERVAL '24' hour) AS volume_24h
            , SUM(t.amount_usd) FILTER(WHERE t.block_time >= now() - INTERVAL '30' day) AS volume_30d
        FROM balancer.trades t
        GROUP BY 1,2
        
        UNION ALL
        
        SELECT 
            t.project_contract_address
            , t.blockchain
            , SUM(t.amount_usd) FILTER(WHERE t.block_time >= now() - INTERVAL '24' hour) AS volume_24h
            , SUM(t.amount_usd) FILTER(WHERE t.block_time >= now() - INTERVAL '30' day) AS volume_30d
        FROM beethoven_x.trades t
        GROUP BY 1,2
        
        UNION ALL
        
        SELECT 
            t.project_contract_address
            , t.blockchain
            , SUM(t.amount_usd) FILTER(WHERE t.block_time >= now() - INTERVAL '24' hour) AS volume_24h
            , SUM(t.amount_usd) FILTER(WHERE t.block_time >= now() - INTERVAL '30' day) AS volume_30d
        FROM jelly_swap.trades t
        GROUP BY 1,2)
        
        
    , revenue AS (
        SELECT 
            f.pool_address
            , f.blockchain
            , SUM(f.protocol_fee_collected_usd) AS revenue
            , SUM(f.protocol_fee_collected_usd) FILTER(WHERE f.day >= now() - INTERVAL '30' day) AS revenue_30d
        FROM balancer.protocol_fee f
        GROUP BY 1,2
        
        UNION ALL
        
         SELECT 
            f.pool_address
            , f.blockchain
            , SUM(f.protocol_fee_collected_usd) AS revenue
            , SUM(f.protocol_fee_collected_usd) FILTER(WHERE f.day >= now() - INTERVAL '30' day) AS revenue_30d
        FROM beethoven_x_fantom.protocol_fee f
        GROUP BY 1,2
        
        UNION ALL
        
          SELECT 
            f.pool_address
            , f.blockchain
            , SUM(f.protocol_fee_collected_usd) AS revenue
            , SUM(f.protocol_fee_collected_usd) FILTER(WHERE f.day >= now() - INTERVAL '30' day) AS revenue_30d
        FROM jelly_swap_sei.protocol_fee f
        GROUP BY 1,2      
    )
    
SELECT 
    w.project
    , SUM(t.tvl) AS tvl
    , SUM(t.tvl) / (SELECT SUM(tvl) FROM tvl) as perc_tvl
    , SUM(s.volume_24h) AS volume_24h
    , SUM(s.volume_24h) / (SELECT SUM(volume_24h) FROM swaps) as perc_vol
    , SUM(s.volume_30d) AS volume_30d
    , SUM(s.volume_24h)/SUM(t.tvl) AS liq_util
    , SUM(f.revenue) AS revenue
FROM pools w
LEFT JOIN tvl t 
    ON t.pool_id = w.poolId 
    AND t.blockchain = w.blockchain 
LEFT JOIN swaps s 
    ON s.project_contract_address = BYTEARRAY_SUBSTRING(w.poolId,1,20) 
    AND s.blockchain = w.blockchain
LEFT JOIN revenue f 
    ON f.pool_address = BYTEARRAY_SUBSTRING(w.poolid,1,20) 
    AND f.blockchain = w.blockchain
GROUP BY 1
ORDER BY 2 DESC