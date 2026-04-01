-- part of a query repo
-- query name: Core Pools Fee Collected by Epoch
-- query link: https://dune.com/queries/3516016


WITH 
pool_labels AS(
    SELECT
        blockchain,
        address,
        pool_type
    FROM labels.balancer_v2_pools
),

dates_cte AS (
    SELECT
        date_sequence AS date
    FROM UNNEST(sequence(date '2023-09-28', CURRENT_DATE, interval '14' day)) AS t(date_sequence)
),

epoch_with_next_change AS (
    SELECT 
        date,
        ROW_NUMBER() OVER(ORDER BY date ASC) AS epoch,
        COALESCE(LEAD(date) OVER(ORDER BY date ASC), CURRENT_DATE) AS next_change_date
    FROM dates_cte
),

daily_epoch AS (
SELECT 
    date AS start_date,
    next_change_date AS end_date,
    date_sequence AS day,
    epoch
FROM epoch_with_next_change
CROSS JOIN UNNEST(sequence(epoch_with_next_change.date, epoch_with_next_change.next_change_date, interval '1' day)) AS t(date_sequence)
)

SELECT
    CONCAT(CAST(e.start_date AS VARCHAR),' - ' ,CAST(e.end_date AS VARCHAR)) AS interval,
        CASE 
            WHEN f.blockchain = 'arbitrum' THEN ' 🟦'
            WHEN f.blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN f.blockchain = 'base' THEN ' 🟨'
            WHEN f.blockchain = 'ethereum' THEN ' Ξ'
            WHEN f.blockchain = 'gnosis' THEN ' 🟩'
            WHEN f.blockchain = 'optimism' THEN ' 🔴'
            WHEN f.blockchain = 'polygon' THEN ' 🟪'
            WHEN f.blockchain = 'polygon' THEN ' 🟣'
        END 
    || ' ' || f.pool_symbol
    AS pool_symbol,
    SUM(f.protocol_fee_collected_usd) AS fees_collected,
    pool_address
FROM daily_epoch e
LEFT JOIN balancer.protocol_fee f ON e.day = f.day
INNER JOIN dune.balancer.dataset_core_pools c ON c.pool = f.pool_id AND c.network = f.blockchain
LEFT JOIN pool_labels l 
ON l.blockchain = f.blockchain AND l.address = f.pool_address
WHERE ('{{Fee Epoch}}' = 'All' OR CAST(CONCAT(CAST(e.start_date AS VARCHAR),' - ' ,CAST(e.end_date AS VARCHAR)) AS VARCHAR) = '{{Fee Epoch}}')
AND ('{{Blockchain}}' = 'All' OR f.blockchain = '{{Blockchain}}')
AND ('{{Pool Address}}' = 'All' OR CAST(pool_address AS VARCHAR) = '{{Pool Address}}')
AND ('{{Pool Type}}' = 'All' OR l.pool_type = '{{Pool Type}}')
GROUP BY 1, 2, 4
HAVING SUM(f.protocol_fee_collected_usd) > 0
ORDER BY 1 DESC, 3 DESC
