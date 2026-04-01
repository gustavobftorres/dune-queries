-- part of a query repo
-- query name: Current veBAL balances (vlAURA providers breakdown)
-- query link: https://dune.com/queries/4025331


SELECT 
    provider,
    SUM(vebal_balance) AS vebal_balance,
    SUM(vebal_balance) / SUM(SUM(vebal_balance)) OVER () AS vebal_pct
FROM query_6753954
GROUP BY 1
ORDER BY 2 DESC