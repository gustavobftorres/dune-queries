-- part of a query repo
-- query name: Core vs. Non-Core Pools Fee Collected by Epoch
-- query link: https://dune.com/queries/3567767


WITH 
pool_labels AS(
    SELECT
        blockchain,
        address,
        CASE WHEN pool_type IN ('WP', 'WP2T')
        THEN 'Weighted'
        WHEN pool_type IN ('SP')
        THEN 'Stable'
        WHEN pool_type IN ('IP')
        THEN 'Investment'   
        WHEN pool_type IN ('MP')
        THEN 'Managed'
        ELSE pool_type
        END AS pool_type
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
    CAST(e.start_date AS TIMESTAMP),
    CASE WHEN c.pool IS NOT NULL 
    THEN 'Core Pool'
    ELSE 'Non-Core Pool'
    END AS is_core,
    SUM(f.protocol_fee_collected_usd) AS fees_collected
FROM daily_epoch e
LEFT JOIN balancer.protocol_fee f ON e.day = f.day
LEFT JOIN dune.balancer.dataset_core_pools c ON c.pool = f.pool_id AND c.network = f.blockchain
LEFT JOIN pool_labels l 
ON l.blockchain = f.blockchain AND l.address = f.pool_address
WHERE ('{{Fee Epoch}}' = 'All' OR CAST(CONCAT(CAST(e.start_date AS VARCHAR),' - ' ,CAST(e.end_date AS VARCHAR)) AS VARCHAR) = '{{Fee Epoch}}')
AND ('{{Blockchain}}' = 'All' OR f.blockchain = '{{Blockchain}}')
AND ('{{Pool Type}}' = 'All' OR l.pool_type = '{{Pool Type}}')
AND e.start_date >= TIMESTAMP '{{Start Date}}'
GROUP BY 1, 2
HAVING SUM(f.protocol_fee_collected_usd) > 0
ORDER BY 1 DESC
