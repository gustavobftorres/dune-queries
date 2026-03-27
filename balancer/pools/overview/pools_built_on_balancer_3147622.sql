-- part of a query repo
-- query name: Pools Built on Balancer
-- query link: https://dune.com/queries/3147622


WITH pools AS (
    SELECT * FROM query_3144841
    )
    
    , tvl AS (
        SELECT 
            l.pool_id
            , BYTEARRAY_SUBSTRING(l.pool_id, 1, 20) AS pool_address
            , l.blockchain
            , SUM(l.pool_liquidity_usd) AS tvl
        FROM balancer.liquidity l
        WHERE l.pool_liquidity_usd > 1 
            AND l.day = (CURRENT_DATE - interval '1' day)
        GROUP BY 1,2,3
        
    UNION ALL
    
        SELECT 
            l.pool_id
            , BYTEARRAY_SUBSTRING(l.pool_id, 1, 20) AS pool_address
            , l.blockchain
            , SUM(l.protocol_liquidity_usd) AS tvl
        FROM beethoven_x_fantom.liquidity l
        WHERE l.protocol_liquidity_usd > 1 
            AND l.day = (CURRENT_DATE - interval '1' day)
                AND protocol_liquidity_usd < 1000000000
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
                AND protocol_liquidity_usd < 1000000000
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
        GROUP BY 1,2       
        )
    
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
    , w.name
    ,  w.blockchain || 
        CASE 
            WHEN w.blockchain = 'arbitrum' THEN ' 🟦 '
            WHEN w.blockchain = 'avalanche_c' THEN ' ⬜  '
            WHEN w.blockchain = 'base' THEN ' 🟨 '
            WHEN w.blockchain = 'ethereum' THEN ' Ξ '
            WHEN w.blockchain = 'fantom' THEN ' 🌐 '
            WHEN w.blockchain = 'gnosis' THEN ' 🟩 '
            WHEN w.blockchain = 'optimism' THEN ' 🔴 '
            WHEN w.blockchain = 'polygon' THEN ' 🟪 '
            WHEN w.blockchain = 'zkevm' THEN ' 🟣 '
            WHEN w.blockchain = 'sei' THEN ' ✇ '           
        END 
    AS blockchain
    , SUM(t.tvl) AS tvl
    , SUM(s.volume_24h) AS volume_24h
    , SUM(s.volume_30d) AS volume_30d
    , SUM(s.volume_24h)/SUM(t.tvl) AS liq_util
    , SUM(f.revenue_30d) AS revenue_30d
    , SUM(f.revenue) AS revenue
    , CONCAT('<a target="_blank" href="https://dune.com/balancer/pool-analysis?1.+Pool+ID_t1b222=', SUBSTRING(CAST(t."pool_id" AS VARCHAR), 1, 66), '&4.+t.Blockchain_t9819b=', t.blockchain, '">View Stats ↗</a>') AS stats
    ,    CASE
            WHEN t.blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/ethereum/pool/', CAST(t."pool_id" AS VARCHAR), '">balancer ↗</a>')
            WHEN t.blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/arbitrum/pool/', CAST(t."pool_id" AS VARCHAR), '">balancer ↗</a>')
            WHEN t.blockchain = 'polygon' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/polygon/pool/', CAST(t."pool_id" AS VARCHAR), '">balancer ↗</a>')
            WHEN t.blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/gnosis-chain/pool/', CAST(t."pool_id" AS VARCHAR), '">balancer ↗</a>')
            WHEN t.blockchain = 'base' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/base/pool/', CAST(t."pool_id" AS VARCHAR), '">balancer ↗</a>')
            WHEN t.blockchain = 'avalanche_c' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/avalanche/pool/', CAST(t."pool_id" AS VARCHAR), '">balancer ↗</a>')
            WHEN t.blockchain = 'optimism' THEN CONCAT('<a target="_blank" href="https://beets.fi/pool/', CAST(t."pool_id" AS VARCHAR), '">beethoven ↗</a>')
            WHEN t.blockchain = 'fantom' THEN CONCAT('<a target="_blank" href="https://beets.fi/pool/', CAST(t."pool_id" AS VARCHAR), '">beethoven ↗</a>')
            WHEN t.blockchain = 'zkevm' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/zkevm/pool/', CAST(t."pool_id" AS VARCHAR), '">balancer ↗</a>')
        END AS pool
    ,    CASE
            WHEN t.blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://etherscan.io/address/0', SUBSTRING(CAST(t."pool_id" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN t.blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://arbiscan.io/address/0', SUBSTRING(CAST(t."pool_id" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN t.blockchain = 'polygon' THEN CONCAT('<a target "_blank" href="https://polygonscan.com/address/0', SUBSTRING(CAST(t."pool_id" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN t.blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://gnosisscan.com/address/0', SUBSTRING(CAST(t."pool_id" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN t.blockchain = 'optimism' THEN CONCAT('<a target="_blank" href="https://optimistic.etherscan.io/address/0', SUBSTRING(CAST(t."pool_id" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN t.blockchain = 'avalanche_c' THEN CONCAT('<a target="_blank" href="https://snowtrace.io/address/0', SUBSTRING(CAST(t."pool_id" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN t.blockchain = 'base' THEN CONCAT('<a target="_blank" href="https://basescan.org/address/0', SUBSTRING(CAST(t."pool_id" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN t.blockchain = 'fantom' THEN CONCAT('<a target="_blank" href="https://ftmscan.com/address/0', SUBSTRING(CAST(t."pool_id" AS VARCHAR), 2, 41), '">⛓</a>')
            WHEN t.blockchain = 'zkevm' THEN CONCAT('<a target "_blank" href="https://zkevm.polygonscan.com/address/0', SUBSTRING(CAST(t."pool_id" AS VARCHAR), 2, 41), '">⛓</a>')
        END AS scan
    , BYTEARRAY_SUBSTRING(w.pool_id,1,20) as pool_address
FROM pools w
LEFT JOIN tvl t 
    ON t.pool_id = w.poolid 
    AND t.blockchain = w.blockchain 
LEFT JOIN swaps s 
    ON s.project_contract_address = BYTEARRAY_SUBSTRING(w.poolId,1,20) 
    AND s.blockchain = w.blockchain
LEFT JOIN revenue f 
    ON f.pool_address = BYTEARRAY_SUBSTRING(w.pool_id,1,20) 
    AND f.blockchain = w.blockchain
GROUP BY 1,2,3,10,11,12,13
ORDER BY 4 DESC