-- part of a query repo
-- query name: veBLA / vlAURA - Monthly Snapshots (last 8 months)
-- query link: https://dune.com/queries/6672388


WITH monthly_targets AS (
    SELECT d AS target_date
    FROM UNNEST(
        SEQUENCE(
            DATE_TRUNC('month', CURRENT_DATE - INTERVAL '7' MONTH),
            DATE_TRUNC('month', CURRENT_DATE),
            INTERVAL '1' MONTH
        )
    ) AS t(d)
),

closest_days AS (
    SELECT 
        m.target_date,
        MIN(q.day) AS actual_day
    FROM monthly_targets m
    JOIN (SELECT DISTINCT day FROM query_601405) q
        ON q.day >= m.target_date 
        AND q.day < m.target_date + INTERVAL '7' DAY
    GROUP BY 1
),

monthly_vebal AS (
    SELECT
        cd.target_date AS month,
        cd.actual_day,
        q.provider,
        SUM(q.vebal_balance) AS vebal_balance
    FROM query_601405 q
    JOIN closest_days cd ON q.day = cd.actual_day
    GROUP BY 1, 2, 3
),

monthly_totals AS (
    SELECT month, SUM(vebal_balance) AS total
    FROM monthly_vebal
    GROUP BY 1
)

SELECT
    v.month,
    v.actual_day AS snapshot_day,
    v.provider,
    v.vebal_balance,
    v.vebal_balance / t.total AS vebal_pct
FROM monthly_vebal v
JOIN monthly_totals t ON v.month = t.month
ORDER BY 1 DESC, 4 DESC