-- part of a query repo
-- query name: veBAL Votes by Provider
-- query link: https://dune.com/queries/2638081


WITH
  parsed_data AS (SELECT * FROM query_2330097), -- Guage Labels
  
  labels AS (SELECT gauge, label AS symbol FROM parsed_data)

SELECT
    end_date,
    COALESCE(
        symbol, 
        CONCAT(
            '0x',
            LOWER(SUBSTRING(to_hex(v.gauge), 1, 4)),
            '...',
            LOWER(SUBSTRING(to_hex(v.gauge), 37, 4))
          )
    ) AS symbol, 
    SUM(vote) AS votes
    FROM
    --vebal_votes
      query_2265987 AS v
    LEFT JOIN labels l
    ON l.gauge = CAST(v.gauge as VARCHAR)
    
WHERE vote > 0 
AND 
  (
    '{{1. Provider}}' = 'All'
    OR CAST(provider AS VARCHAR(42)) = '{{1. Provider}}'
    )
GROUP BY 1, 2
ORDER BY end_date DESC, votes DESC 