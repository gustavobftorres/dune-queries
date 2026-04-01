-- part of a query repo
-- query name: Core Pools Epoch
-- query link: https://dune.com/queries/3516138


WITH dates_cte AS (
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

SELECT DISTINCT
    CONCAT(CAST(e.start_date AS VARCHAR),' - ' ,CAST(e.end_date AS VARCHAR)) AS interval,
    CAST(e.epoch AS VARCHAR) AS epoch
FROM daily_epoch e    

UNION ALL

SELECT 
    'All',
    'All'
    
ORDER BY 1 DESC