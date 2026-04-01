-- part of a query repo
-- query name: Liquidity Growth vs. Emissions Variation correlation, by blockchain
-- query link: https://dune.com/queries/4041730


SELECT
    month,
    blockchain,
    CASE 
        WHEN mom_liquidity_growth <= -0.20 THEN 'Significant Decrease'
        WHEN mom_liquidity_growth > -0.20 AND mom_liquidity_growth < -0.05 THEN 'Moderate Decrease'
        WHEN mom_liquidity_growth >= -0.05 AND mom_liquidity_growth <= 0.05 THEN 'Stable'
        WHEN mom_liquidity_growth > 0.05 AND mom_liquidity_growth <= 0.20 THEN 'Moderate Increase'
        WHEN mom_liquidity_growth > 0.20 THEN 'Significant Increase'
    END AS liquidity_growth_tier,
    APPROX_PERCENTILE(mom_liquidity_growth, 0.5) AS median_mom_liquidity_growth,
    APPROX_PERCENTILE(mom_emissions_growth, 0.5) AS median_mom_emissions_growth,
    CORR(mom_liquidity_growth, mom_emissions_growth) AS correlation_emissions_liquidity_growth
FROM query_4039839
WHERE monthly_emissions IS NOT NULL
AND mom_liquidity_growth IS NOT NULL
AND IS_FINITE(mom_liquidity_growth)
AND month < DATE_TRUNC('month', now())
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2 ASC, 4 DESC