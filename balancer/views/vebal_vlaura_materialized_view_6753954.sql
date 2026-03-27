-- part of a query repo
-- query name: Vebal vlAura - Materialized View
-- query link: https://dune.com/queries/6753954


-- Materialized view: vebal_vlaura_join
SELECT 
    provider,
    vebal_balance
FROM query_601405
WHERE provider != 'Aura'
AND day = CURRENT_DATE

UNION ALL

SELECT
    CAST(provider AS VARCHAR),
    SUM(vebal_balance) AS vebal_balance
FROM query_4023187
WHERE vebal_round = (SELECT MAX(vebal_round) - 1 FROM query_4023187)
GROUP BY 1