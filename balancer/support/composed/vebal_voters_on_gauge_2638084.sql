-- part of a query repo
-- query name: veBAL Voters on Gauge
-- query link: https://dune.com/queries/2638084


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
        CONCAT(
                    '<a target="_blank" href="https://etherscan.io/address/0x',
                    LOWER(to_hex(provider)),
                    '">',
                    CONCAT(
                    '0x',
                    LOWER(SUBSTRING(to_hex(provider), 1, 4)),
                    '...',
                    LOWER(SUBSTRING(to_hex(provider), 37, 4))
                    ),
                    '↗</a>'
                )
             AS symbol,
      SUM(vote) AS votes
    FROM
      --vebal_votes
      query_2265987 v
    WHERE
      (
        '{{Gauge}}' = 'All'
        OR CAST(v.gauge AS VARCHAR(42)) = '{{Gauge}}'
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
    GROUP BY 1, 2,3
    ORDER BY 4 DESC
    LIMIT 10
  )
    
SELECT
  CAST(end_date as TIMESTAMP) as end_date, 
  COALESCE(t.wallet_address_short, 'Others') AS wallet_address,
  COALESCE(symbol, 'Others'),
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
    '{{Gauge}}' = 'All'
    OR CAST(v.gauge AS VARCHAR(42)) = '{{Gauge}}'
  )
GROUP BY
  1,
  2,
  3
ORDER BY
  1 DESC,
  4 DESC