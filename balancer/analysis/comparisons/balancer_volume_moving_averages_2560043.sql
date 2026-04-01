-- part of a query repo
-- query name: Balancer Volume Moving Averages
-- query link: https://dune.com/queries/2560043


WITH
    daily_vol AS (
        SELECT
            DATE_TRUNC('day', block_time) AS day,
            SUM(amount_usd) AS volume
        FROM
            balancer.trades
        GROUP BY 1
    ),

    sma AS (
        SELECT
            day,
            volume,
            SUM(volume) OVER (ORDER BY day ASC ROWS BETWEEN 19 PRECEDING AND CURRENT ROW ) / 20 AS "20d Vol SMA",
            SUM(volume) OVER (ORDER BY day ASC ROWS BETWEEN 49 PRECEDING AND CURRENT ROW ) / 50 AS "50d Vol SMA",
            SUM(volume) OVER (ORDER BY day ASC ROWS BETWEEN 99 PRECEDING AND CURRENT ROW ) / 100 AS "100d Vol SMA",
            SUM(volume) OVER (ORDER BY day ASC ROWS BETWEEN 199 PRECEDING AND CURRENT ROW ) / 200 AS "200d Vol SMA"
        FROM
            daily_vol
        )

SELECT * FROM sma WHERE day >= current_date - interval '{{Date Range in Days}}' day
