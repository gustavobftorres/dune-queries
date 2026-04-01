-- part of a query repo
-- query name: Liquidity on YB Stablecoin Pools, Top 5 Pools on Arbitrum
-- query link: https://dune.com/queries/3814803


WITH labels AS (
        SELECT
            pool_address AS address,
            pool_symbol AS name,
            blockchain
        FROM balancer.liquidity
        WHERE token_address IN (SELECT token_address FROM query_3814790)
        AND blockchain = 'arbitrum'
        GROUP BY 1, 2, 3
),

tvl AS (
    SELECT
        day,
        p.blockchain,
        SUBSTRING(CAST(pool_id AS VARCHAR), 1, 42) AS pool_id,
        l.name AS pool_symbol,
        CASE WHEN '{{Stablecoins TVL Currency}}' = 'USD'
            THEN SUM(protocol_liquidity_usd) 
        WHEN '{{Stablecoins TVL Currency}}' = 'ETH'
            THEN SUM(protocol_liquidity_eth) 
        END AS tvl
    FROM balancer.liquidity p
        LEFT JOIN labels l ON p.pool_address = l.address AND p.blockchain = l.blockchain
    WHERE
        day <= TIMESTAMP '{{End date}}'
        AND day >= TIMESTAMP '{{Start date}}'
        AND day <= (SELECT max(day) FROM balancer.liquidity WHERE version = '2')
        AND l.address IS NOT NULL
        AND p.blockchain = 'arbitrum'
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
        WHEN p.pool_id IS NOT NULL THEN 
        COALESCE(t.pool_symbol, SUBSTRING(p.pool_id,3,8))
        ELSE '(Others)'
    END AS pool,
    SUM(t.tvl) AS "TVL"
FROM tvl t
LEFT JOIN top_pools p ON p.pool_id = t.pool_id AND p.blockchain = t.blockchain
LEFT JOIN total_tvl tt ON tt.day = t.day
WHERE t.day >= TIMESTAMP '{{Start date}}'
GROUP BY 1, 2,3
ORDER BY 1, 4 DESC