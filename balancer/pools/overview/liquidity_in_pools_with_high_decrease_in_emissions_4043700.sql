-- part of a query repo
-- query name: Liquidity in Pools with high decrease in emissions
-- query link: https://dune.com/queries/4043700


SELECT *
FROM query_4039839
WHERE mom_emissions_growth < -0.2
AND monthly_emissions > 100