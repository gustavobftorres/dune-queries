-- part of a query repo
-- query name: CoWAMM analysis per pool
-- query link: https://dune.com/queries/4708152


WITH pools AS(
SELECT
    DATE_TRUNC('month', day) AS month,
    blockchain,
    pool_address,
    pool_symbol,
    APPROX_PERCENTILE(tvl_usd, 0.5) AS median_tvl,
    SUM(volume) AS total_volume,
    SUM(surplus) AS total_surplus,
    SUM(volume) / SUM(surplus) AS ratio
FROM query_4404883
WHERE (CASE WHEN blockchain IN ('gnosis', 'ethereum') THEN tvl_usd > 50000
WHEN blockchain IN ('arbitrum', 'base') THEN tvl_usd > 10000 END)
GROUP BY 1, 2, 3, 4)

SELECT 
    p.*,
    q.ratio AS chain_average_ratio,
    CASE WHEN p.ratio < q.ratio
    THEN 1
    ELSE 0
    END AS cash_cow --pools with above average surplus per dollar in volume in it's chain
FROM pools p 
LEFT JOIN query_4708077 q
ON p.blockchain = q.blockchain AND p.month = q.month
WHERE p.month < TIMESTAMP '2025-02-01 00:00'
ORDER BY 1 DESC, 5 DESC 