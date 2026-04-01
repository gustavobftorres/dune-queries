-- part of a query repo
-- query name: unpriced trades on bv3
-- query link: https://dune.com/queries/4445360


SELECT * FROM dex.trades
WHERE block_date = TIMESTAMP '2024-12-17'
AND project = 'balancer' AND version = '3'
AND amount_usd IS NULL
