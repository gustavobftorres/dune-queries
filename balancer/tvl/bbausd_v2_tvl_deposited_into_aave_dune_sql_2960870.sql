-- part of a query repo
-- query name: bbaUSD v2 TVL Deposited into Aave (Dune SQL)
-- query link: https://dune.com/queries/2960870


SELECT
  CAST(day as timestamp) as day,
  CASE
    WHEN token_symbol LIKE 'a%' THEN 'aTokens'
    ELSE 'Stablecoins'
  END AS kind,
  SUM(pool_liquidity_usd)
FROM
  balancer_v2_ethereum.liquidity
WHERE
  pool_id IN (
    0x2f4eb100552ef93840d5adc30560e5513dfffacb000000000000000000000334,
    0x82698aecc9e28e9bb27608bd52cf57f704bd1b83000000000000000000000336,
    0xae37d54ae477268b9997d4161b96b8200755935c000000000000000000000337
  )
  AND day >= CAST('2022-09-20' AS TIMESTAMP)
GROUP BY
  1,
  2