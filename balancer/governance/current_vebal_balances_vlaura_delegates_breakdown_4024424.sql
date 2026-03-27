-- part of a query repo
-- query name: Current veBAL balances (vlAURA delegates breakdown)
-- query link: https://dune.com/queries/4024424


WITH vebal_vlaura_join AS(
SELECT 
    provider,
    vebal_balance
FROM query_601405 qa
WHERE qa.provider != 'Aura'
AND qa.day = CURRENT_DATE

UNION ALL

SELECT
    CAST(delegate AS VARCHAR),
    SUM(vebal_balance)
FROM query_4023187
GROUP BY 1)

SELECT 
    provider,
    SUM(vebal_balance) AS vebal_balance,
    SUM(vebal_balance / (SELECT SUM(vebal_balance) FROM vebal_vlaura_join)) AS vebal_pct
FROM vebal_vlaura_join
GROUP BY 1

