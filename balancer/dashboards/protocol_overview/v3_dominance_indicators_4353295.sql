-- part of a query repo
-- query name: V3 Dominance Indicators
-- query link: https://dune.com/queries/4353295


WITH v3 AS (
    SELECT 
        block_date,
        version,
        SUM(tvl_usd) AS daily_tvl_usd,
        SUM(tvl_eth) AS daily_tvl_eth,
        SUM(swap_amount_usd) AS daily_volume_usd,
        SUM(fee_amount_usd) AS daily_fees_usd
    FROM balancer.pools_metrics_daily
    WHERE block_date >= TIMESTAMP '2024-11-29 00:00'
      AND ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
      AND version = '3'
    GROUP BY 1, 2
),

all_versions AS (
    SELECT
        block_date,
        SUM(tvl_usd) AS total_tvl_usd,
        SUM(tvl_eth) AS total_tvl_eth,
        SUM(swap_amount_usd) AS total_volume_usd,
        SUM(fee_amount_usd) AS total_fees_usd
    FROM balancer.pools_metrics_daily
    WHERE block_date >= TIMESTAMP '2024-11-29 00:00'
      AND ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
    GROUP BY 1
)

SELECT 
    v3.block_date,
    v3.version,
    v3.daily_tvl_usd,
    v3.daily_tvl_eth,
    v3.daily_volume_usd,
    v3.daily_fees_usd,
    all_versions.total_tvl_usd,
    all_versions.total_tvl_eth,
    all_versions.total_volume_usd,
    all_versions.total_fees_usd,
    (v3.daily_tvl_usd / all_versions.total_tvl_usd) * 100 AS tvl_dominance_pct,
    (v3.daily_tvl_eth / all_versions.total_tvl_eth) * 100 AS tvl_eth_dominance_pct,
    (v3.daily_volume_usd / all_versions.total_volume_usd) * 100 AS volume_dominance_pct,
    (v3.daily_fees_usd / all_versions.total_fees_usd) * 100 AS fees_dominance_pct
FROM v3
JOIN all_versions ON v3.block_date = all_versions.block_date
ORDER BY 1 DESC
