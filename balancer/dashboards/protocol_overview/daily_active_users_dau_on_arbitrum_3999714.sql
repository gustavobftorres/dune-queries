-- part of a query repo
-- query name: Daily Active Users (DAU) on Arbitrum
-- query link: https://dune.com/queries/3999714


WITH daily_activity AS (
    SELECT
        'arbitrum' AS blockchain,
        DATE_TRUNC('day', block_time) AS day,
        COUNT(DISTINCT "from") AS dau
    FROM arbitrum.transactions
    WHERE "to" = 0xBA12222222228d8Ba445958a75a0704d566BF2C8
    AND block_time >= TIMESTAMP '{{Start date}}'
    AND block_time <= TIMESTAMP '{{End date}}'
    GROUP BY 1, 2
)
SELECT
    blockchain,
    day,
    dau,
    AVG(dau) OVER (
        ORDER BY day
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS rolling_14d_avg_dau
FROM daily_activity
ORDER BY day

