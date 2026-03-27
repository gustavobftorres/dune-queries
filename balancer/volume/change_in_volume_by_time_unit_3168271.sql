-- part of a query repo
-- query name: Change in Volume by Time Unit
-- query link: https://dune.com/queries/3168271


SELECT 
    day
    , CAST(day AS TIMESTAMP) AS day_timestamp
    , CASE WHEN month(current_date) < 10 THEN substring(date_format(CAST(day AS TIMESTAMP), '%m-%d'), 2, 4)
           ELSE date_format(CAST(day AS TIMESTAMP), '%m-%d') 
    END AS formatted_day
    , volume
    , lag(volume) OVER(ORDER BY day) AS lag_volume
    , (volume - lag(volume) OVER(ORDER BY day)) / lag(volume) OVER(ORDER BY day) AS delta
    , ((volume - lag(volume) OVER(ORDER BY day)) / lag(volume) OVER(ORDER BY day)) * 100 AS delta_percentage
    , CASE
        WHEN (volume - lag(volume) OVER(ORDER BY day)) / lag(volume) OVER(ORDER BY day) > 0 THEN '+'
        ELSE '-'
    END AS pos_neg
FROM (
    SELECT 
        distinct
        date_trunc('{{Time Unit}}', block_date) AS day, 
        sum(amount_usd) OVER(PARTITION BY date_trunc('{{Time Unit}}', block_date)) AS volume
    FROM balancer.trades
    WHERE 
        CASE 
            WHEN '{{Time Unit}}' = 'DAY' THEN block_date >= current_date - (interval '{{Date Range in Time Units}}' DAY + interval '1' DAY)
            WHEN '{{Time Unit}}' = 'WEEK' THEN block_date >= date_trunc('{{Time Unit}}', current_date) - ((interval '{{Date Range in Time Units}}' DAY) * 7 + (interval '1' DAY * 7))
            WHEN '{{Time Unit}}' = 'MONTH' THEN block_date >= date_trunc('{{Time Unit}}', current_date) - (interval '{{Date Range in Time Units}}' MONTH + interval '1' MONTH)
            WHEN '{{Time Unit}}' = 'QUARTER' THEN block_date >= date_trunc('{{Time Unit}}', current_date) - ((interval '{{Date Range in Time Units}}' MONTH * 3) + (interval '1' MONTH * 3))
        END
)

ORDER BY day ASC, delta