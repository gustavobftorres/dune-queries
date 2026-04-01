-- part of a query repo
-- query name: Fraxtal Swaps
-- query link: https://dune.com/queries/3899966


WITH
  swaps AS (
    SELECT
        CAST(day AS TIMESTAMP) AS day,
        AVG(amount_usd) AS avg_volume,
        SUM(swaps_count) AS txns,
        SUM(amount_usd) AS volume
    FROM
        dune.balancer.dataset_fraxtal_snapshots
    WHERE CAST(day AS TIMESTAMP) >= TIMESTAMP '{{1. Start date}}'
    GROUP BY 1
)

SELECT
    day,
    avg_volume,
    txns AS "Swaps",
    volume AS "Volume",
    SUM(volume) OVER (ORDER BY day ASC) AS "All-Time Volume",
    volume / txns AS "Avg. Volume per Swap",
    SUM(volume) OVER (ORDER BY day ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW ) / 30 AS "30d Vol SMA",
    SUM(volume) OVER (ORDER BY day ASC ROWS BETWEEN 49 PRECEDING AND CURRENT ROW ) / 50 AS "50d Vol SMA",
    SUM(volume) OVER (ORDER BY day ASC ROWS BETWEEN 99 PRECEDING AND CURRENT ROW ) / 100 AS "100d Vol SMA",
    SUM(volume) OVER (ORDER BY day ASC ROWS BETWEEN 199 PRECEDING AND CURRENT ROW ) / 200 AS "200d Vol SMA"
FROM
    swaps