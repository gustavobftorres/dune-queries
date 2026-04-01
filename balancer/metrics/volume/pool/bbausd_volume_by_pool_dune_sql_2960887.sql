-- part of a query repo
-- query name: bbaUSD Volume by Pool (Dune SQL)
-- query link: https://dune.com/queries/2960887


SELECT
  DATE_TRUNC('month', block_time) AS month,
  CASE
    WHEN project_contract_address = 0xa13a9247ea42d743238089903570127dda72fe44 THEN 'bbaUSD v2'
    ELSE 'bbaUSD v1'
  END AS name,
  SUM(amount_usd)
FROM
  dex.trades
WHERE
  blockchain = 'ethereum'
  AND project = 'balancer'
  AND project_contract_address IN (
    0xa13a9247ea42d743238089903570127dda72fe44,
    0x7b50775383d3d6f0215a8f290f2c9e2eebbeceb2
  )
GROUP BY
  1,
  2