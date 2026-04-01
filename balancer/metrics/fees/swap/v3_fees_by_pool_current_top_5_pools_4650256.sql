-- part of a query repo
-- query name: V3 Fees by Pool, Current Top 5 Pools
-- query link: https://dune.com/queries/4650256


WITH labels AS (
    SELECT * FROM (
        SELECT
            address,
            name,
            blockchain,
            ROW_NUMBER() OVER (PARTITION BY address, blockchain ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.balancer_v3_pools
        GROUP BY 1, 2, 3
    ) WHERE num = 1
),

fee AS (
    SELECT
        day,
        p.blockchain,
        SUBSTRING(CAST(pool_id AS VARCHAR), 1, 42) AS pool_id,
        l.name AS pool_symbol,
        SUM(protocol_fee_collected_usd) AS fee
    FROM balancer.protocol_fee p
        LEFT JOIN labels l ON p.pool_address = l.address AND p.blockchain = l.blockchain
    WHERE
        ('{{blockchain}}' = 'All' or p.blockchain = '{{blockchain}}')
        AND version = '3'
        GROUP BY 1, 2, 3, 4
),

total_fee AS (
    SELECT
        day,
        'Total' AS pool_id,
        SUM(fee) AS total_fee
    FROM fee
    GROUP BY 1, 2
    ORDER BY 1 DESC
),

top_pools AS (
    SELECT
        DISTINCT pool_id,
        t.blockchain,
        t.pool_symbol,
        fee
    FROM fee t
    LEFT JOIN labels l ON CAST(t.pool_id AS VARCHAR) = CAST(l.address AS VARCHAR) 
        AND t.blockchain = l.blockchain
    WHERE
        day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
        AND fee IS NOT NULL
    ORDER BY 4 DESC
    LIMIT 5
)

SELECT
    CAST(t.day AS TIMESTAMP) AS day,
    p.pool_id,
    CASE
        WHEN p.pool_id IS NOT NULL THEN CONCAT(CASE 
            WHEN t.blockchain = 'arbitrum' THEN ' 🟦 |'
            WHEN t.blockchain = 'avalanche_c' THEN ' ⬜  |'
            WHEN t.blockchain = 'base' THEN ' 🟨 |'
            WHEN t.blockchain = 'ethereum' THEN ' Ξ |'
            WHEN t.blockchain = 'gnosis' THEN ' 🟩 |'
            WHEN t.blockchain = 'optimism' THEN ' 🔴 |'
            WHEN t.blockchain = 'polygon' THEN ' 🟪 |'
            WHEN t.blockchain = 'zkevm' THEN ' 🟣 |'
        END 
        , ' ', COALESCE(t.pool_symbol, SUBSTRING(p.pool_id,3,8)))
        ELSE '(Others)'
    END AS pool,
    SUM(t.fee) AS "fee"
FROM fee t
LEFT JOIN top_pools p ON p.pool_id = t.pool_id AND p.blockchain = t.blockchain
LEFT JOIN total_fee tt ON tt.day = t.day
GROUP BY 1, 2,3
ORDER BY 1, 4 DESC