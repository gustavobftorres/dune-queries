-- part of a query repo
-- query name: MoM Liquidity Growth vs. BAL emissions Correlation Analysis
-- query link: https://dune.com/queries/4040450


SELECT 
    blockchain,
    pool_address,
    pool_symbol,
    SUM(monthly_emissions) AS bal_emissions,
    APPROX_PERCENTILE(mom_liquidity_growth, 0.5) AS median_mom_liquidity_growth,
    APPROX_PERCENTILE(mom_emissions_growth, 0.5) AS median_mom_emissions_growth,
    CORR(mom_liquidity_growth, mom_emissions_growth) AS correlation_emissions_liquidity_growth
FROM query_4039839
WHERE monthly_emissions IS NOT NULL
AND mom_liquidity_growth IS NOT NULL
AND IS_FINITE(mom_liquidity_growth)
GROUP BY 1, 2, 3
ORDER BY 4 DESC
