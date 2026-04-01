-- part of a query repo
-- query name: Balancer Weekly Volume by Version
-- query link: https://dune.com/queries/2611976


/* Volume per week */
/* Visualization: bar chart */
WITH
  swaps AS (
    SELECT
        DATE_TRUNC('week', block_time) AS week,
        version,
        AVG(amount_usd) AS avg_volume,
        COUNT(*) AS txns,
        SUM(amount_usd) AS volume
    FROM
        balancer.trades
        WHERE block_time >=   TIMESTAMP '{{1. Start date}}'         
        AND ('{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}')
    GROUP BY 1, 2
)

SELECT
    s.week,
    version,
    avg_volume,
    txns AS "Swaps",
    volume AS "Volume",
    volume / txns AS "Avg. Volume per Swap",
    SUM(volume) OVER (ORDER BY s.week ASC ROWS BETWEEN 19 PRECEDING AND CURRENT ROW ) / 20 AS "20d Vol SMA",
    SUM(volume) OVER (ORDER BY s.week ASC ROWS BETWEEN 49 PRECEDING AND CURRENT ROW ) / 50 AS "50d Vol SMA",
    SUM(volume) OVER (ORDER BY s.week ASC ROWS BETWEEN 99 PRECEDING AND CURRENT ROW ) / 100 AS "100d Vol SMA",
    SUM(volume) OVER (ORDER BY s.week ASC ROWS BETWEEN 199 PRECEDING AND CURRENT ROW ) / 200 AS "200d Vol SMA"
FROM
    swaps s
    WHERE s.week >= TIMESTAMP '{{1. Start date}}' 