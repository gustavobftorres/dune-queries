-- part of a query repo
-- query name: Balancer Token Price (USD)
-- query link: https://dune.com/queries/31190


SELECT date_trunc('day', minute) AS day, AVG(price) AS "Price"
FROM prices.usd
WHERE contract_address = '\xba100000625a3754423978a60c9317c58a424e3d'
GROUP BY 1