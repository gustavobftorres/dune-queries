-- part of a query repo
-- query name: Balancer Stats by Version
-- query link: https://dune.com/queries/4353152


WITH daily_sums AS (
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
    GROUP BY 1, 2
)

SELECT 
    CASE 
        WHEN '{{aggregation}}' = 'daily' THEN block_date
        WHEN '{{aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
        WHEN '{{aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
    END AS block_date,
    version,
    APPROX_PERCENTILE(daily_tvl_usd, 0.5) AS tvl_usd_median,
    APPROX_PERCENTILE(daily_tvl_eth, 0.5) AS tvl_eth_median,
    SUM(daily_volume_usd) AS total_volume_usd,
    SUM(daily_fees_usd) AS total_fees_usd
FROM daily_sums
GROUP BY 1, 2;
