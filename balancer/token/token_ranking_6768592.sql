-- part of a query repo
-- query name: Token ranking
-- query link: https://dune.com/queries/6768592


SELECT
    token,
    RANK() OVER (ORDER BY SUM(CASE WHEN date_trunc('month', "date") = date_trunc('month', CURRENT_DATE - INTERVAL '1' MONTH) THEN volume END) DESC NULLS LAST) AS position
FROM query_6754098
WHERE ('{{4. blockchain}}' = 'All' OR blockchain = '{{4. blockchain}}')
  AND ('{{5. version}}' = 'All' OR version = '{{5. version}}')
GROUP BY token