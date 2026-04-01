-- part of a query repo
-- query name: Liquidity on LST Pools, Top 5 Pools
-- query link: https://dune.com/queries/3373184


WITH labels AS (
        SELECT
            pool_address AS address,
            SUBSTRING(name,9,100) AS name,
            blockchain
        FROM dune.balancer.result_lst_pools
        GROUP BY 1, 2, 3
),

tvl AS (
    SELECT
        day,
        p.blockchain,
        SUBSTRING(CAST(pool_id AS VARCHAR), 1, 42) AS pool_id,
        l.name AS pool_symbol,
        CASE WHEN '{{4. Currency}}' = 'USD'
        THEN SUM(protocol_liquidity_usd) 
        WHEN '{{4. Currency}}' = 'eth'
        THEN SUM(protocol_liquidity_eth) 
        END AS tvl
    FROM balancer.liquidity p
        LEFT JOIN labels l ON p.pool_address = l.address AND p.blockchain = l.blockchain
    WHERE
        day <= TIMESTAMP '{{2. End date}}'
        AND day >= TIMESTAMP '{{1. Start date}}'
        AND day <= (SELECT max(day) FROM balancer.liquidity WHERE version = '2')
        AND l.address IS NOT NULL
        AND protocol_liquidity_usd < 1000000000
        AND ('{{3. Blockchain}}' = 'All' OR p.blockchain = '{{3. Blockchain}}')
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
        day = (SELECT max(day) FROM tvl)
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
WHERE t.day >= TIMESTAMP '{{1. Start date}}'
GROUP BY 1, 2,3
ORDER BY 1, 4 DESC