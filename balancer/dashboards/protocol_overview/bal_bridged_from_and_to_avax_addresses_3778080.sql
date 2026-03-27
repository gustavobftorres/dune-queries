-- part of a query repo
-- query name: BAL bridged from and to AVAX - addresses
-- query link: https://dune.com/queries/3778080


SELECT q.blockchain, user_address, sum(amount_original) AS bal_bridged
FROM query_3777544 q
GROUP BY 1, 2
ORDER BY 3 DESC