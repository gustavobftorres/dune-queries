-- part of a query repo
-- query name: veBAL Holders
-- query link: https://dune.com/queries/2318569


WITH
  locked_at AS (
    SELECT
      wallet_address,
      MIN(day) AS locked_at
    FROM
     --vebal_balances_day
      query_2276840
    WHERE
      vebal_balance > 0
    GROUP BY
      1
  ),
  info AS (
    SELECT
      wallet_address,
      CONCAT(
        '0x',
        LOWER(SUBSTRING(to_hex(wallet_address), 1, 4)),
        '...',
        LOWER(SUBSTRING(to_hex(wallet_address), 37, 4))
      ) AS wallet_address_short,
      lock_time,
      bpt_balance,
      vebal_balance
    FROM
      --vebal_balances_day
      query_2276840
    WHERE
      day = CURRENT_DATE
    ORDER BY
      5 DESC NULLS FIRST
  )
SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      vebal_balance DESC NULLS FIRST
  ) AS ranking,
  CONCAT(
    '<a target="_blank" href="https://etherscan.io/address/0x',
    LOWER(to_hex(i.wallet_address)),
    '">',
    wallet_address_short,
    '↗</a>'
  ) AS wallet_address,
  CAST(locked_at AS DATE) AS locked_at,
  bpt_balance /* lock_time / (365*86400/12) AS lock_time, */,
  vebal_balance,
  CONCAT(
    '<a href="https://dune.com/balancer/vebal-analysis?1.+Wallet+Address_tef185=0x',
    LOWER(to_hex(i.wallet_address)),
    '">view stats</a>'
  ) AS stats
FROM
  info AS i
  JOIN locked_at AS l ON l.wallet_address = i.wallet_address
  AND vebal_balance > 0