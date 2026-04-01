-- part of a query repo
-- query name: Fees Collected on LST/LRT Pools
-- query link: https://dune.com/queries/3921278


WITH labels AS (
        SELECT
            pool_address AS address,
            name,
            blockchain
        FROM dune.balancer.result_lst_pools
        GROUP BY 1, 2, 3
),

fees AS (
    SELECT
        day,
        p.blockchain,
        SUBSTRING(CAST(pool_id AS VARCHAR), 1, 42) AS pool_id,
        ol.name AS pool_symbol,
        SUM(protocol_fee_collected_usd) AS fees
    FROM balancer.protocol_fee p
        LEFT JOIN labels l ON p.pool_address = l.address AND p.blockchain = l.blockchain
        LEFT JOIN labels.balancer_v2_pools_optimism ol ON l.address = ol.address 
        AND l.blockchain = ol.blockchain
    WHERE
        day <= TIMESTAMP '{{End date}}'
        AND day >= TIMESTAMP '{{Start date}}'
        AND day <= (SELECT max(day) FROM balancer.protocol_fee WHERE version = '2')
        AND l.address IS NOT NULL
        AND p.blockchain = 'optimism'
    GROUP BY 1, 2, 3, 4
),

total_fees AS (
    SELECT
        day,
        'Total' AS pool_id,
        SUM(fees) AS total_fees
    FROM fees
    GROUP BY 1, 2
    ORDER BY 1 DESC
),

top_pools AS (
    SELECT
        DISTINCT pool_id,
        t.blockchain,
        t.pool_symbol,
        fees
    FROM fees t
    LEFT JOIN labels l ON CAST(t.pool_id AS VARCHAR) = CAST(l.address AS VARCHAR) 
        AND t.blockchain = l.blockchain
    WHERE
        day = (SELECT max(day) FROM fees)
        AND fees IS NOT NULL
    ORDER BY 4 DESC
    LIMIT 20
)

SELECT
    CAST(t.day AS TIMESTAMP) AS day,
    p.pool_id,
    CASE
        WHEN p.pool_id IS NOT NULL THEN 
        COALESCE(t.pool_symbol, SUBSTRING(p.pool_id,3,8))
        ELSE '(Others)'
    END AS pool,
    SUM(t.fees) AS "fees"
FROM fees t
LEFT JOIN top_pools p ON p.pool_id = t.pool_id AND p.blockchain = t.blockchain
LEFT JOIN total_fees tt ON tt.day = t.day
WHERE t.day >= TIMESTAMP '{{Start date}}'
GROUP BY 1, 2,3
ORDER BY 1, 4 DESC