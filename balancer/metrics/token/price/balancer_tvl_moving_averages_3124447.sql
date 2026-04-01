-- part of a query repo
-- query name: Balancer TVL Moving Averages
-- query link: https://dune.com/queries/3124447


WITH
    usd_tvl AS (
        SELECT
            day,
            SUM(protocol_liquidity_usd) AS tvl
        FROM
            balancer.liquidity
        WHERE
            day <= (SELECT MAX(day) FROM balancer.liquidity WHERE version = '2')
        GROUP BY 1
    ),
    
    eth_tvl AS (
        SELECT
            day,
            SUM(protocol_liquidity_eth) AS tvl
        FROM
            balancer.liquidity
        WHERE
            day <= (SELECT MAX(day) FROM balancer.liquidity WHERE version = '2')
        GROUP BY 1
    ),

    combined_tvl AS (
        SELECT
            day,
            tvl
        FROM
            usd_tvl
        WHERE
            '{{Currency}}' = 'USD'
        UNION ALL
        SELECT
            day,
            tvl
        FROM
            eth_tvl
        WHERE
            '{{Currency}}' = 'eth'
    ),

    sma AS (
        SELECT
            CAST(day AS TIMESTAMP) AS day,
            tvl,
            SUM(tvl) OVER (ORDER BY day ASC ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) / 20 AS "20d TVL SMA",
            SUM(tvl) OVER (ORDER BY day ASC ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) / 50 AS "50d TVL SMA",
            SUM(tvl) OVER (ORDER BY day ASC ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) / 100 AS "100d TVL SMA",
            SUM(tvl) OVER (ORDER BY day ASC ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) / 200 AS "200d TVL SMA"
        FROM
            combined_tvl
    )

SELECT * FROM sma WHERE day >= current_date - interval '{{Date Range in Days}}' day