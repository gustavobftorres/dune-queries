-- part of a query repo
-- query name: Fees on LST Pools, Top 5 Pools
-- query link: https://dune.com/queries/3446122


WITH labels AS (
        SELECT
            pool_address AS address,
            SUBSTRING(name,9,100) AS name,
            blockchain
        FROM dune.balancer.result_lst_pools
        GROUP BY 1, 2, 3
),

fee AS (
    SELECT
        CASE WHEN '{{5. Aggregation}}' = 'Monthly'
        THEN DATE_TRUNC('month', p.day)
        WHEN '{{5. Aggregation}}' = 'Weekly' 
        THEN DATE_TRUNC('week', p.day) 
        WHEN '{{5. Aggregation}}' = 'Daily' 
        THEN DATE_TRUNC('day', p.day) 
        END AS date,
        p.blockchain,
        SUBSTRING(CAST(pool_id AS VARCHAR), 1, 42) AS pool_id,
        l.name AS pool_symbol,
        CASE WHEN '{{4. Currency}}' = 'USD'
        THEN SUM(protocol_fee_collected_usd) 
        WHEN '{{4. Currency}}' = 'eth'
        THEN SUM(protocol_fee_collected_usd / e.median_price_eth) 
        END AS fee
    FROM balancer.protocol_fee p
        LEFT JOIN labels l ON p.pool_address = l.address AND p.blockchain = l.blockchain
        LEFT JOIN dune.balancer.result_eth_price e ON e.day = p.day
    WHERE
        p.day <= TIMESTAMP '{{2. End date}}'
        AND p.day >= TIMESTAMP '{{1. Start date}}'
        AND l.address IS NOT NULL
        AND ('{{3. Blockchain}}' = 'All' OR p.blockchain = '{{3. Blockchain}}')
    GROUP BY 1, 2, 3, 4
),

total_fee AS (
    SELECT
        date,
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
        sum(fee) as fee
    FROM fee t
    LEFT JOIN labels l ON CAST(t.pool_id AS VARCHAR) = CAST(l.address AS VARCHAR) 
        AND t.blockchain = l.blockchain
    GROUP BY 1,2,3
    ORDER BY 4 DESC
    LIMIT 5
)

SELECT
    CAST(t.date AS TIMESTAMP) AS day,
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
LEFT JOIN total_fee tt ON tt.date = t.date
WHERE t.date >= TIMESTAMP '{{1. Start date}}'
GROUP BY 1, 2,3
ORDER BY 1, 4 DESC