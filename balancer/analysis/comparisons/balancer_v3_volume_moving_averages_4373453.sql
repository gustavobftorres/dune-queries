-- part of a query repo
-- query name: Balancer V3 Volume Moving Averages
-- query link: https://dune.com/queries/4373453


WITH
  swaps AS (
    SELECT
    CASE 
        WHEN '{{aggregation}}' = 'daily' THEN block_date
        WHEN '{{aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
        WHEN '{{aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
    END AS day,
        AVG(amount_usd) AS avg_volume,
        COUNT(*) AS txns,
        SUM(amount_usd) AS volume
    FROM
        balancer.trades
    WHERE block_time >= CAST('2021-09-01 00:00:00' AS TIMESTAMP)
        AND block_time <= CAST(NOW() AS TIMESTAMP)
        AND ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
        AND (version = '3')
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