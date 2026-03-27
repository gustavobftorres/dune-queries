-- part of a query repo
-- query name: BAL Price
-- query link: https://dune.com/queries/2552320


SELECT price FROM prices.usd WHERE contract_address = 0xba100000625a3754423978a60c9317c58a424e3d ORDER BY minute DESC LIMIT 1