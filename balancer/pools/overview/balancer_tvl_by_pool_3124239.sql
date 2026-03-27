-- part of a query repo
-- query name: Balancer TVL by Pool
-- query link: https://dune.com/queries/3124239


WITH 
    usd_tvl AS (
        SELECT 
            pool_id,
            CAST(day AS TIMESTAMP) AS day,
            SUM(protocol_liquidity_usd) AS pool_tvl,
            SUM(protocol_liquidity_usd) FILTER(WHERE x.day = y.latest_day) AS latest_tvl,
            MAX(latest_day) AS latest_day
        FROM balancer.liquidity x
        LEFT JOIN (SELECT MAX(day) - INTERVAL '1' DAY AS latest_day FROM balancer.liquidity) y
            ON y.latest_day = x.day
        WHERE day >= current_date - INTERVAL '{{Date Range in Days}}' DAY
            AND day <= (SELECT MAX(day) FROM balancer.liquidity WHERE version = '2')
        GROUP BY 1, 2
    ),
    
    eth_tvl AS (
        SELECT 
            pool_id,
            CAST(day AS TIMESTAMP) AS day,
            SUM(protocol_liquidity_eth) AS pool_tvl,
            SUM(protocol_liquidity_eth) FILTER(WHERE x.day = y.latest_day) AS latest_tvl,
            MAX(latest_day) AS latest_day
        FROM balancer.liquidity x
        LEFT JOIN (SELECT MAX(day) AS latest_day FROM balancer.liquidity WHERE version = '2') y
            ON y.latest_day = x.day
        WHERE day >= current_date - INTERVAL '{{Date Range in Days}}' DAY
            AND day <= (SELECT MAX(day) FROM balancer.liquidity WHERE version = '2')
        GROUP BY 1, 2
    ),

    combined_data AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY day ORDER BY pool_tvl DESC) AS day_rank
        FROM (
            SELECT * FROM usd_tvl WHERE '{{Currency}}' = 'USD'
            UNION ALL
            SELECT * FROM eth_tvl WHERE '{{Currency}}' = 'eth'
        ) combined)

    , custom_labels AS (
        SELECT
            address,
            blockchain,
            kind,
            name
        FROM
            query_2846430
    )

    , top_rank AS (
        SELECT 
            x.*,
            y.blockchain,
            CASE 
                WHEN y.blockchain = 'arbitrum' THEN '🟦 |'
                WHEN y.blockchain = 'avalanche_c' THEN '⬜ |'
                WHEN y.blockchain = 'base' THEN '🟨 |'
                WHEN y.blockchain = 'ethereum' THEN 'Ξ  |'
                WHEN y.blockchain = 'gnosis' THEN '🟩 |'
                WHEN y.blockchain = 'optimism' THEN '🔴 |'
                WHEN y.blockchain = 'polygon' THEN '🟪 |'
                WHEN y.blockchain = 'zkevm' THEN '🟣 |'
            END || ' ' || ' ' || COALESCE(
                cl.name,
                l.name,
                CAST(bytearray_substring(x.pool_id, 1, 2) AS VARCHAR) || '...' || SUBSTRING(CAST(x.pool_id AS VARCHAR), 39, 4)
            ) AS sym
        FROM combined_data x
        LEFT JOIN (SELECT DISTINCT blockchain, pool_id FROM balancer.liquidity) y
            ON y.pool_id = x.pool_id
        LEFT JOIN labels.balancer_v2_pools l 
            ON y.blockchain = l.blockchain 
            AND l.address = bytearray_substring(x.pool_id, 1, 20)
        LEFT JOIN custom_labels cl 
            ON y.blockchain = cl.blockchain 
            AND cl.address = bytearray_substring(x.pool_id, 1, 20)
        WHERE day_rank <= CAST('{{Pool Rank by Daily TVL}}' AS INTEGER)
    )

    , others AS (
        SELECT 
            NULL AS pool_id,
            CAST(day AS TIMESTAMP) AS day,
            SUM(pool_tvl) AS pool_tvl,
            SUM(latest_tvl) AS latest_tvl,
            MAX(latest_day) AS latest_day,
            NULL AS day_rank,
            NULL AS blockchain,
            'others' AS sym
        FROM (
            SELECT 
                day,
                MAX(latest_day) AS latest_day,
                SUM(pool_tvl) AS pool_tvl,
                SUM(latest_tvl) AS latest_tvl
            FROM combined_data
            WHERE day_rank > CAST('{{Pool Rank by Daily TVL}}' AS INTEGER)
            GROUP BY day
        ) aggregated_data
    GROUP BY day
    )
    
SELECT * FROM top_rank
UNION
SELECT * FROM others
ORDER BY day DESC, day_rank ASC NULLS FIRST
