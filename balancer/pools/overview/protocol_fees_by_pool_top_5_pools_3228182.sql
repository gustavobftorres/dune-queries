-- part of a query repo
-- query name: Protocol Fees by Pool, top 5 pools
-- query link: https://dune.com/queries/3228182


WITH
    fees AS (
    SELECT
        day,
        p.blockchain,
        SUBSTRING(CAST(pool_id AS VARCHAR), 1, 42) AS pool_id,
        pool_symbol,
        SUM(protocol_fee_collected_usd) AS fees
    FROM balancer.protocol_fee p
    LEFT JOIN dune.balancer.dataset_core_pools c 
    ON c.network = p.blockchain AND c.pool = p.pool_id
    WHERE ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
    AND ('{{Pool Type}}' = 'All' OR p.pool_type = '{{Pool Type}}')
    AND protocol_fee_collected_usd < 1000000000
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
        SUM(fees)
    FROM fees t
    WHERE fees IS NOT NULL 
    AND day >= TIMESTAMP '{{Start Date}}'
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 5
)

SELECT
    CASE WHEN '{{Aggregation}}' = 'Monthly'
    THEN DATE_TRUNC('month', t.day) 
    WHEN '{{Aggregation}}' = 'Weekly'
    THEN DATE_TRUNC('week', t.day)
    WHEN '{{Aggregation}}' = 'Daily'
    THEN DATE_TRUNC('day', t.day)
    END AS _date,
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
        , ' ', t.pool_symbol)
        ELSE '(Others)'
    END AS pool,
    SUM(t.fees) AS "fees"
FROM fees t
LEFT JOIN top_pools p ON p.pool_id = t.pool_id AND p.blockchain = t.blockchain
LEFT JOIN total_fees tt ON tt.day = t.day
WHERE ('{{Blockchain}}' = 'All' OR t.blockchain = '{{Blockchain}}')
AND t.day >= TIMESTAMP '{{Start Date}}'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
