-- part of a query repo
-- query name: Balancer Weekly Volume
-- query link: https://dune.com/queries/2655636


/* Volume per week */
/* Visualization: bar chart */
WITH
  swaps AS (
    SELECT
        DATE_TRUNC('week', block_time) AS week,
        AVG(amount_usd) AS avg_volume,
        COUNT(*) AS txns,
        SUM(amount_usd) AS volume
    FROM
        dex.trades
    WHERE/*
        blockchain = 'ethereum'
        AND*/ project = 'balancer'
    GROUP BY 1
)

SELECT
    week,
    avg_volume,
    txns AS "Swaps",
    volume AS "Volume",
    volume / txns AS "Avg. Volume per Swap",
    SUM(volume) OVER (ORDER BY week ASC ROWS BETWEEN 19 PRECEDING AND CURRENT ROW ) / 20 AS "20d Vol SMA",
    SUM(volume) OVER (ORDER BY week ASC ROWS BETWEEN 49 PRECEDING AND CURRENT ROW ) / 50 AS "50d Vol SMA",
    SUM(volume) OVER (ORDER BY week ASC ROWS BETWEEN 99 PRECEDING AND CURRENT ROW ) / 100 AS "100d Vol SMA",
    SUM(volume) OVER (ORDER BY week ASC ROWS BETWEEN 199 PRECEDING AND CURRENT ROW ) / 200 AS "200d Vol SMA"
FROM
    swaps
    WHERE week >= now() - interval '3' month