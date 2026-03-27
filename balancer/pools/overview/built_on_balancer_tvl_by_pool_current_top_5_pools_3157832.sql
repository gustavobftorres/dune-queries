-- part of a query repo
-- query name: Built on Balancer TVL by Pool, Current Top 5 Pools
-- query link: https://dune.com/queries/3157832


WITH labels AS (
    SELECT * FROM (
        SELECT
            BYTEARRAY_SUBSTRING(pool_id,1,20) AS address,
            name,
            blockchain
        FROM query_3144841
        WHERE project = '{{Project}}'
        GROUP BY 1, 2, 3
    )
),

tvl AS (
    SELECT
        t.day,
        t.blockchain,
        SUBSTRING(CAST(t.pool_id AS VARCHAR), 1, 42) AS pool_id,
        l.name,
        SUM(t.pool_liquidity_usd) AS tvl
    FROM balancer.liquidity t 
        LEFT JOIN labels l ON BYTEARRAY_SUBSTRING(t.pool_id,1,20) = l.address 
        AND t.blockchain = l.blockchain
    WHERE
        day >= TIMESTAMP '{{Start Date}}'
        AND name IS NOT NULL
    GROUP BY 1, 2, 3, 4
   
   UNION ALL
    
    SELECT
        t.day,
        t.blockchain,
        SUBSTRING(CAST(t.pool_id AS VARCHAR), 1, 42) AS pool_id,
        l.name,
        SUM(t.protocol_liquidity_usd) AS tvl
    FROM beethoven_x_fantom.liquidity t 
        LEFT JOIN labels l ON BYTEARRAY_SUBSTRING(t.pool_id,1,20) = l.address 
        AND t.blockchain = l.blockchain
    WHERE
        day >= TIMESTAMP '{{Start Date}}'
        AND name IS NOT NULL
    GROUP BY 1, 2, 3, 4
    
   UNION ALL
    
    SELECT
        t.day,
        t.blockchain,
        SUBSTRING(CAST(t.pool_id AS VARCHAR), 1, 42) AS pool_id,
        l.name,
        SUM(t.protocol_liquidity_usd) AS tvl
    FROM jelly_swap_sei.liquidity t 
        LEFT JOIN labels l ON BYTEARRAY_SUBSTRING(t.pool_id,1,20) = l.address 
        AND t.blockchain = l.blockchain
    WHERE
        day >= TIMESTAMP '{{Start Date}}'
        AND name IS NOT NULL
    GROUP BY 1, 2, 3, 4    
    ),

total_tvl AS (
    SELECT
        day,
        'Total' AS pool_id,
        SUM(tvl) AS total_tvl
    FROM tvl
    GROUP BY 1, 2
    ORDER BY 1 DESC
),

top_pools AS (
    SELECT
        DISTINCT pool_id,
        t.blockchain,
        tvl
    FROM tvl t
    LEFT JOIN labels l ON CAST(t.pool_id AS VARCHAR) = CAST(l.address AS VARCHAR) 
        AND t.blockchain = l.blockchain
    WHERE
        day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
        AND tvl IS NOT NULL
    ORDER BY 3 DESC
    LIMIT 5
)

SELECT
    CAST(t.day AS TIMESTAMP) AS day,
    CASE
        WHEN p.pool_id IS NOT NULL THEN CONCAT( 
        CASE 
            WHEN t.blockchain = 'arbitrum' THEN ' 🟦 |'
            WHEN t.blockchain = 'avalanche_c' THEN ' ⬜  |'
            WHEN t.blockchain = 'base' THEN ' 🟨 |'
            WHEN t.blockchain = 'ethereum' THEN ' Ξ |'
            WHEN t.blockchain = 'fantom' THEN ' 🌐 |'
            WHEN t.blockchain = 'gnosis' THEN ' 🟩 |'
            WHEN t.blockchain = 'optimism' THEN ' 🔴 |'
            WHEN t.blockchain = 'polygon' THEN ' 🟪 |'
            WHEN t.blockchain = 'zkevm' THEN ' 🟣 |'
            WHEN t.blockchain = 'sei' THEN ' ✇ |'            
        END 
        , ' ', t.name)
        ELSE '(Others)'
    END AS pool,
    SUM(t.tvl) AS "TVL"
FROM tvl t
LEFT JOIN top_pools p ON p.pool_id = t.pool_id AND p.blockchain = t.blockchain
LEFT JOIN total_tvl tt ON tt.day = t.day
WHERE t.day >= TIMESTAMP '{{Start Date}}'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
