-- part of a query repo
-- query name: TVL by Pool, Current Top 5 Pools (Pools Overview)
-- query link: https://dune.com/queries/2872964


WITH labels AS (
    SELECT * FROM (
        SELECT
            address,
            name,
            blockchain,
            ROW_NUMBER() OVER (PARTITION BY address, blockchain ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.addresses
        WHERE category IN ('balancer_v1_pool', 'balancer_v2_pool')
        GROUP BY 1, 2, 3
    ) WHERE num = 1
),

tvl AS (
    SELECT
        day,
        p.blockchain,
        SUBSTRING(CAST(pool_id AS VARCHAR), 1, 42) AS pool_id,
        l.name AS pool_symbol,
        CASE WHEN '{{5. Currency}}' = 'USD'
        THEN SUM(pool_liquidity_usd)
        WHEN '{{5. Currency}}' = 'eth'
        THEN SUM(pool_liquidity_eth)
        END AS tvl
    FROM balancer.liquidity p
        LEFT JOIN labels l ON p.pool_address = l.address AND p.blockchain = l.blockchain
    WHERE
        day <= TIMESTAMP '{{3. End date}}'
        AND day >= TIMESTAMP '{{2. Start date}}'
        AND day < CURRENT_DATE
        AND protocol_liquidity_usd < 1000000000
        AND ('{{4. Blockchain}}' = 'All' OR p.blockchain = '{{4. Blockchain}}')
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
        t.pool_symbol,
        tvl
    FROM tvl t
    LEFT JOIN labels l ON CAST(t.pool_id AS VARCHAR) = CAST(l.address AS VARCHAR) 
        AND t.blockchain = l.blockchain
    WHERE
        day = date_trunc('day', current_timestamp) - INTERVAL '1' DAY
        AND tvl IS NOT NULL
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
    SUM(t.tvl) AS "TVL"
FROM tvl t
LEFT JOIN top_pools p ON p.pool_id = t.pool_id AND p.blockchain = t.blockchain
LEFT JOIN total_tvl tt ON tt.day = t.day
WHERE t.day >= TIMESTAMP '{{2. Start date}}'
GROUP BY 1, 2,3
ORDER BY 1, 4 DESC