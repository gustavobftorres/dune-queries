-- part of a query repo
-- query name: Total veBAL
-- query link: https://dune.com/queries/559677


SELECT day, SUM(vebal_balance) AS total_vebal
FROM balancer_ethereum.vebal_balances_day
WHERE day = CURRENT_DATE
GROUP BY 1
ORDER BY 1 DESC
LIMIT 1
