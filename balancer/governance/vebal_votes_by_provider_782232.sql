-- part of a query repo
-- query name: veBAL Votes by Provider
-- query link: https://dune.com/queries/782232


WITH
  parsed_data AS (SELECT CAST(address AS VARCHAR) AS gauge, name AS label FROM labels.balancer_gauges), -- Gauge Labels
  
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
      balancer_ethereum.vebal_votes AS v
    LEFT JOIN labels l
    ON l.gauge = CAST(v.gauge as VARCHAR)
    
WHERE vote > 0 
AND 
  (
    '{{Provider}}' = 'All'
    OR CAST(provider AS VARCHAR(42)) = '{{Provider}}'
    )
GROUP BY 1, 2
ORDER BY end_date DESC, votes DESC 

/*WITH
  parsed_data AS (SELECT * FROM query_2330097), -- Guage Labels
  labels AS (SELECT gauge, label AS symbol FROM parsed_data),
  total_votes AS (
    SELECT
      end_date,
      CAST(provider AS VARCHAR(42)) AS provider,
      SUM(vote) AS total_vote
    FROM
      query_2265987
    WHERE vote > 0 
    GROUP BY 1, 2
  )

SELECT
  v.end_date,
  COALESCE(
    CASE
      --WHEN t.total_vote IS NULL THEN 'others'
      WHEN v.vote / t.total_vote > 0.1 THEN l.symbol
      ELSE 'others'
    END, 
    CONCAT(
      '0x',
      LOWER(SUBSTRING(to_hex(v.gauge), 1, 4)),
      '...',
      LOWER(SUBSTRING(to_hex(v.gauge), 37, 4))
    )
  ) AS symbol, 
  SUM(v.vote) AS votes
FROM
  query_2265987 AS v
LEFT JOIN labels l
  ON l.gauge = CAST(v.gauge AS VARCHAR)
LEFT JOIN total_votes t
  ON v.end_date = t.end_date AND CAST(v.provider AS VARCHAR(42)) = t.provider

WHERE vote > 0 
AND (
  '{{Provider}}' = 'All'
  OR CAST(v.provider AS VARCHAR(42)) = '{{Provider}}'
)
GROUP BY 1, 2
ORDER BY v.end_date DESC, votes DESC;*/
