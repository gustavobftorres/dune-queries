-- part of a query repo
-- query name: vebal balance by day
-- query link: https://dune.com/queries/4047969


SELECT day, sum(vebal_balance) AS vebal_balance
FROM query_601405
GROUP BY 1