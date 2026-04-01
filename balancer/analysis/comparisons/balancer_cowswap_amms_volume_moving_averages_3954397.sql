-- part of a query repo
-- query name: Balancer CowSwap AMMs Volume Moving Averages
-- query link: https://dune.com/queries/3954397


WITH
  swaps AS (
    SELECT
        DATE_TRUNC('day', block_time) AS day,
        AVG(amount_usd) AS avg_volume,
        COUNT(*) AS txns,
        SUM(amount_usd) AS volume
    FROM
        balancer_cowswap_amm.trades
    WHERE block_time >= CAST('2021-09-01 00:00:00' AS TIMESTAMP)
        AND block_time <= CAST(NOW() AS TIMESTAMP)
        AND ('{{2. Blockchain}}' = 'All' OR blockchain = '{{2. Blockchain}}')
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