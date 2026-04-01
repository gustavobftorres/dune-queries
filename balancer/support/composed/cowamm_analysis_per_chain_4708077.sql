-- part of a query repo
-- query name: CoWAMM analysis per chain
-- query link: https://dune.com/queries/4708077


SELECT
    DATE_TRUNC('month', day) AS month,
    blockchain,
    APPROX_PERCENTILE(tvl_usd, 0.5) AS median_tvl,
    SUM(volume) AS total_volume,
    SUM(surplus) AS total_surplus,
    SUM(volume) / SUM(surplus) AS ratio
FROM query_4404883
WHERE (CASE WHEN blockchain IN ('gnosis', 'ethereum') THEN tvl_usd > 50000
WHEN blockchain IN ('arbitrum', 'base') THEN tvl_usd > 10000 END)
GROUP BY 1, 2
ORDER BY 1 DESC, 5 DESC 