-- part of a query repo
-- query name: veBAL Power by Provider
-- query link: https://dune.com/queries/2318623


SELECT
  day,
  SUM(vebal_balance) AS vebal_balance
FROM
 --vebal_balances_day
  query_2276840
WHERE
  (
    '{{1. Wallet Address}}' = 'All'
    OR CAST(wallet_address AS VARCHAR(42)) = '{{1. Wallet Address}}'
  )
GROUP BY
  1