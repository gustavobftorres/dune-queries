-- part of a query repo
-- query name: veBAL Holders Overview
-- query link: https://dune.com/queries/6672376


SELECT 
    provider,
    vebal_balance,
    vebal_pct
FROM query_4025331
ORDER BY 2 DESC
LIMIT 100