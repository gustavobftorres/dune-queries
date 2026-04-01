-- part of a query repo
-- query name: Emissions decrease impact on liquidity
-- query link: https://dune.com/queries/4043590


SELECT *
FROM query_4040450
WHERE median_mom_emissions_growth < -0.20
AND bal_emissions > 100