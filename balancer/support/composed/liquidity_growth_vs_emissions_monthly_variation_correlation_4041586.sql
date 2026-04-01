-- part of a query repo
-- query name: Liquidity Growth vs. Emissions Monthly Variation correlation
-- query link: https://dune.com/queries/4041586


SELECT
    month,
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
WHERE mom_liquidity_growth IS NOT NULL
AND IS_FINITE(mom_liquidity_growth)
AND month < DATE_TRUNC('month', now())
GROUP BY 1, 2
HAVING SUM(monthly_emissions) > 100
ORDER BY 3 DESC