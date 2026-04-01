-- part of a query repo
-- query name: veBAL by Top LPs
-- query link: https://dune.com/queries/2283067


WITH
  total_vebal AS (
    SELECT
      day,
      SUM(vebal_balance) AS total_vebal
    FROM
    --vebal_balances_day
      query_2276840
    GROUP BY
      1
  ),
  top_providers AS (
    SELECT
      wallet_address,
      LOWER(CONCAT(
        '0x',
        SUBSTRING(to_hex(wallet_address), 1, 4),
        '...',
        SUBSTRING(to_hex(wallet_address), 37, 4)
      )) AS short_provider
    FROM
    --vebal_balances_day
      query_2276840
    WHERE
      day = CURRENT_DATE
    ORDER BY
      vebal_balance DESC
    LIMIT
      10
  )
SELECT
  r.day,
  CASE
    WHEN t.wallet_address = 0xaf52695e1bb01a16d33d7194c28c42b10e0dbec2 THEN 'Aura'
    WHEN t.wallet_address = 0x9cc56fa7734da21ac88f6a816af10c5b898596ce THEN 'Tetu'
    ELSE COALESCE(t.short_provider, 'Others')
  END AS provider,
  SUM(vebal_balance) AS vebal_balance,
  SUM(vebal_balance) / total_vebal AS pct
FROM
--vebal_balances_day
  query_2276840 AS r
  INNER JOIN total_vebal AS v ON v.day = r.day
  LEFT JOIN top_providers AS t ON t.wallet_address = r.wallet_address
GROUP BY
  1,
  2,
  total_vebal
ORDER BY
  1 DESC,
  3 DESC