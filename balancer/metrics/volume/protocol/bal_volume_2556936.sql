-- part of a query repo
-- query name: BAL Volume
-- query link: https://dune.com/queries/2556936


SELECT SUM(amount_usd) AS "BAL Volume" FROM dex.trades 
WHERE (token_bought_address = 0xba100000625a3754423978a60c9317c58a424e3d 
OR token_sold_address = 0xba100000625a3754423978a60c9317c58a424e3d)
AND block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '7' DAY
UNION ALL
SELECT SUM(amount_usd) AS "BAL Volume" FROM dex.trades 
WHERE (token_bought_address = 0xba100000625a3754423978a60c9317c58a424e3d 
OR token_sold_address = 0xba100000625a3754423978a60c9317c58a424e3d)
AND block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '1' DAY