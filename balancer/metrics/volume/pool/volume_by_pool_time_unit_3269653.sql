-- part of a query repo
-- query name: Volume by Pool & Time Unit
-- query link: https://dune.com/queries/3269653


WITH pool_ranking AS (
    SELECT 
        DATE_TRUNC('day', block_time) AS block_date,
        blockchain,
        CASE 
            WHEN a.blockchain = 'arbitrum' THEN ' 🟦 |'
            WHEN a.blockchain = 'avalanche_c' THEN ' ⬜  |'
            WHEN a.blockchain = 'base' THEN ' 🟨 |'
            WHEN a.blockchain = 'ethereum' THEN ' Ξ |'
            WHEN a.blockchain = 'gnosis' THEN ' 🟩 |'
            WHEN a.blockchain = 'optimism' THEN ' 🔴 |'
            WHEN a.blockchain = 'polygon' THEN ' 🟪 |'
            WHEN a.blockchain = 'zkevm' THEN ' 🟣 |'
        END || pool_symbol AS pool_symbol,
        pool_id,
        SUM(amount_usd) AS volume
    FROM balancer.trades a
    WHERE block_time >= NOW() - INTERVAL '30' day
    GROUP BY 1, 2, 3, 4
),

ranked_pools AS (
    SELECT
        blockchain,
        pool_id,
        pool_symbol,
        SUM(volume),
        RANK() OVER (ORDER BY SUM(volume) DESC) AS rank
    FROM pool_ranking
    GROUP BY 1, 2, 3
)

SELECT 
    block_date,
    p.blockchain,
    COALESCE(r.pool_symbol, 'Others') AS pool_label,
    SUM(p.volume) AS total_volume
FROM pool_ranking p
LEFT JOIN ranked_pools r ON r.pool_id = p.pool_id
AND r.blockchain = p.blockchain
AND r.rank <= 10
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;
