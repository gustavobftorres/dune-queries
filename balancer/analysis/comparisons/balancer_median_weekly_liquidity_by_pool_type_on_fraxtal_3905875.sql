-- part of a query repo
-- query name: Balancer Median Weekly Liquidity by Pool Type on Fraxtal
-- query link: https://dune.com/queries/3905875


WITH liquidity AS (
    SELECT 
        CAST(day AS TIMESTAMP) AS day,
        pool_type,
        SUM(CAST(protocol_liquidity_usd AS double)) AS tvl
    FROM dune.balancer.dataset_fraxtal_snapshots
    WHERE CAST(day AS TIMESTAMP) >= TIMESTAMP '{{1. Start date}}' 
    GROUP BY 1, 2
    )

    SELECT
        CAST(DATE_TRUNC('week', day) AS timestamp) AS week,
        pool_type,
        APPROX_PERCENTILE(tvl, 0.5) AS median_liquidity
    FROM liquidity
    GROUP BY 1, 2
