-- part of a query repo
-- query name: veBAL Power by Provider
-- query link: https://dune.com/queries/782249


SELECT
  day,
  SUM(vebal_balance) AS vebal_balance
FROM
 --vebal_balances_day
  balancer_ethereum.vebal_balances_day
WHERE
  (
    '{{Provider}}' = 'All'
    OR CAST(wallet_address AS VARCHAR(42)) = '{{Provider}}'
  )
  AND day <= CURRENT_DATE
GROUP BY
  1