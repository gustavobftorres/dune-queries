-- part of a query repo
-- query name: Balancer Volume by Token (Dune SQL)
-- query link: https://dune.com/queries/2829170


SELECT
    CASE 
        WHEN '{{1. Aggregation}}' = 'Daily'   THEN "date"
        WHEN '{{1. Aggregation}}' = 'Weekly'  THEN date_trunc('week', "date")
        WHEN '{{1. Aggregation}}' = 'Monthly' THEN date_trunc('month', "date")
    END AS "date",
    CASE WHEN r.position <= 10 THEN s.token ELSE 'Others' END AS token,
    SUM(s.volume) AS volume
FROM query_6754098 s
LEFT JOIN query_6768592 r ON r.token = s.token
WHERE ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
  AND "date" >= TIMESTAMP '{{2. Start date}}'
  AND "date" <= TIMESTAMP '{{3. End date}}'
  AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
GROUP BY 1, 2
ORDER BY 1, 2