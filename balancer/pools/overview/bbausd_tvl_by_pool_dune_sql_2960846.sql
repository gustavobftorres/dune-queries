-- part of a query repo
-- query name: bbaUSD TVL by Pool (Dune SQL)
-- query link: https://dune.com/queries/2960846


SELECT
  TRY_CAST(day AS TIMESTAMP) AS day,
  CASE
    WHEN pool_id = 0xa13a9247ea42d743238089903570127dda72fe4400000000000000000000035d
    THEN 'bbaUSD v2'
    ELSE 'bbaUSD v1'
  END AS name,
  SUM(pool_liquidity_usd) AS tvl
FROM balancer_v2_ethereum.liquidity
WHERE
  pool_id IN (0xa13a9247ea42d743238089903570127dda72fe4400000000000000000000035d, 0x7b50775383d3d6f0215a8f290f2c9e2eebbeceb20000000000000000000000fe)
GROUP BY
  1,
  2