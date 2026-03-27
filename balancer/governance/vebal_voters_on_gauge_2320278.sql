-- part of a query repo
-- query name: veBAL Voters on Gauge
-- query link: https://dune.com/queries/2320278


WITH
  top_providers AS (
    SELECT
      provider AS wallet_address,
      CONCAT(
          '0x',
          LOWER(SUBSTRING(to_hex(provider), 1, 4)),
          '...',
          LOWER(SUBSTRING(to_hex(provider), 37, 4))
        ) AS wallet_address_short,
      SUM(vote) AS votes
    FROM
      --vebal_votes
      query_2265987 v
    WHERE
      (
        '{{2. Gauge}}' = 'All'
        OR CAST(v.gauge AS VARCHAR(42)) = '{{2. Gauge}}'
      )
    AND
      end_date = (
        SELECT
          end_date
        FROM
          --vebal_votes
          query_2265987
        WHERE
          start_date <= CURRENT_DATE
        AND
          end_date >= CURRENT_DATE
        LIMIT 1
      )
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 10
  )
    
SELECT
  end_date, 
  COALESCE(t.wallet_address_short, 'Others') AS wallet_address,
  SUM(vote) AS votes
FROM
  --vebal_votes
  query_2265987 v
LEFT JOIN
  top_providers t
ON
  t.wallet_address = v.provider
WHERE
  (
    '{{2. Gauge}}' = 'All'
    OR CAST(v.gauge AS VARCHAR(42)) = '{{2. Gauge}}'
  )
GROUP BY
  1,
  2
ORDER BY
  1 DESC,
  3 DESC