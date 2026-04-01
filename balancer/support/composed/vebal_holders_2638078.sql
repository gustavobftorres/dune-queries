-- part of a query repo
-- query name: veBAL Holders
-- query link: https://dune.com/queries/2638078


WITH
  locked_at AS (
    SELECT
      wallet_address as provider,
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
      wallet_address as provider,
      CONCAT(
        '0x',
        LOWER(SUBSTRING(to_hex(wallet_address), 1, 4)),
        '...',
        LOWER(SUBSTRING(to_hex(wallet_address), 37, 4))
      ) AS provider_short,
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
  ),
  ranking as (
    SELECT
      ROW_NUMBER() OVER (
        ORDER BY
          vebal_balance DESC NULLS FIRST
      ) AS ranking,
      CONCAT(
        '<a target="_blank" href="https://etherscan.io/address/0x',
        LOWER(to_hex(i.provider)),
        '">',
        provider_short,
        '↗</a>'
      ) AS provider_link,
      i.provider as provider,
      CAST(locked_at AS DATE) AS locked_at,
      bpt_balance /* lock_time / (365*86400/12) AS lock_time, */,
      vebal_balance,
      CONCAT(
        '<a target="_blank" href="https://dune.com/balancer/wipvebal-analysis?1.+Provider_t62387=0x',
        LOWER(to_hex(i.provider)),
        '">view stats</a>'
      ) AS stats
    FROM
      info AS i
      JOIN locked_at AS l ON l.provider = i.provider
      AND vebal_balance > 0
    )
    
SELECT * FROM ranking 
    WHERE 
      (
        '{{1. Provider}}' = 'All'
        OR CAST(provider AS VARCHAR) = '{{1. Provider}}'
      )