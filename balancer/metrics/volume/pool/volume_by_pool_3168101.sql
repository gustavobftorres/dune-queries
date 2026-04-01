-- part of a query repo
-- query name: Volume by Pool
-- query link: https://dune.com/queries/3168101


WITH pool_ranking AS (
    SELECT 
        blockchain,
        CASE WHEN a.blockchain = 'arbitrum' THEN ' 🟦 |'
            WHEN a.blockchain = 'avalanche_c' THEN ' ⬜  |'
            WHEN a.blockchain = 'base' THEN ' 🟨 |'
            WHEN a.blockchain = 'ethereum' THEN ' Ξ |'
            WHEN a.blockchain = 'gnosis' THEN ' 🟩 |'
            WHEN a.blockchain = 'optimism' THEN ' 🔴 |'
            WHEN a.blockchain = 'polygon' THEN ' 🟪 |'
            WHEN a.blockchain = 'zkevm' THEN ' 🟣 |'
            END || pool_symbol 
            AS pool_symbol,
        pool_type,
        SUM(amount_usd) AS volume
    FROM balancer.trades a
    WHERE block_time >= NOW() - INTERVAL '24' hour
    GROUP BY 1, 2, 3
),

ranked_pools AS (
    SELECT
        blockchain,
        pool_symbol,
        pool_type,
        volume,
        RANK() OVER (PARTITION BY blockchain ORDER BY volume DESC) AS rank
    FROM pool_ranking
)

SELECT 
    blockchain,
    CASE 
        WHEN rank <= 10 THEN pool_symbol
        ELSE 'Others'
    END AS pool_label,
    SUM(volume) AS total_volume
FROM ranked_pools
GROUP BY 1, 2
ORDER BY 2 DESC;
