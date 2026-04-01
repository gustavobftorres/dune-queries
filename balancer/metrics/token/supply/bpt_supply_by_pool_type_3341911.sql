-- part of a query repo
-- query name: BPT supply by pool type
-- query link: https://dune.com/queries/3341911


-- TESTING ON 
--https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-arbitrum-v2/graphql?query=%7B%0A%09pool%28%0A++++id%3A+%220x7c82a23b4c48d796dee36a9ca215b641c6a8709d000000000000000000000acd%22%0A++%29+%7B%0A%09++snapshots%28%0A++++++orderBy%3A+timestamp%0A++++++orderDirection%3A+desc%0A++++%29+%7B%0A%09++++timestamp%0A++++++totalShares%0A%09++%7D%0A%09%7D%0A%7D

-- WEIGHTED POOLS ----> OK
/*SELECT
  block_day AS day,
  token_address AS token,
  SUM(amount_raw) / POWER(10, 18) AS supply
FROM balances_arbitrum.erc20_day
WHERE
  token_address = 0x32df62dc3aed2cd6224193052ce665dc18165841
  AND wallet_address != 0x0000000000000000000000000000000000000000
GROUP BY 1, 2
ORDER BY 1 DESC*/

-- LINEAR POOLS -----> Presenting negative values, even with joins/exits logic
--bpt supply por joins e exits e depois remover preminted

/*WITH premints AS (SELECT
  block_day AS day,
  token_address AS token,
  (SUM(amount_raw) - CAST('5192296858534827628530496329220095' AS INT256))
  / POWER(10, 18) AS supply
FROM balances_arbitrum.erc20_day
WHERE
  token_address = 0xbd724eb087d4cc0f61a5fed1fffaf937937e14de
  AND wallet_address != 0x0000000000000000000000000000000000000000
GROUP BY 1, 2
ORDER BY 1 DESC),

joins AS(
SELECT DATE_TRUNC('day', evt_block_time) as block_date, SUM(amountIn/POWER(10,18)) as ajoins
FROM balancer_v2_arbitrum.Vault_evt_Swap
WHERE tokenIn = 0xbd724eb087d4cc0f61a5fed1fffaf937937e14de
GROUP BY 1),

exits AS(
SELECT DATE_TRUNC('day', evt_block_time) as block_date, SUM(amountOut/POWER(10,18)) as aexits
FROM balancer_v2_arbitrum.Vault_evt_Swap
WHERE tokenOut = 0xbd724eb087d4cc0f61a5fed1fffaf937937e14de
GROUP BY 1),

joins_and_exits AS(
SELECT j.block_date, SUM(COALESCE(ajoins,0) - COALESCE(aexits,0)) OVER (ORDER BY j.block_date ASC) AS delta
FROM joins j
FULL OUTER JOIN exits e ON j.block_date = e.block_date
)

SELECT p.day, SUM(p.supply + COALESCE(delta,0) - (CAST('5192296858534827628530496329220095' AS INT256)/ POWER(10,18)))
FROM premints p
LEFT JOIN joins_and_exits j ON p.day = j.block_date
GROUP BY 1*/

-- Stable, Composable Stable and Managed Pools -----> ok
WITH premints AS (
SELECT poolId AS pool_id, t.token, d.delta/POWER(10,18) AS premints,
ROW_NUMBER() OVER (PARTITION BY poolId ORDER BY evt_block_time ASC) AS rn
FROM balancer_v2_arbitrum.Vault_evt_PoolBalanceChanged pb
CROSS JOIN UNNEST (pb.deltas) WITH ORDINALITY d(delta, i)
CROSS JOIN UNNEST (pb.tokens) WITH ORDINALITY t(token, i)
WHERE d.i = t.i AND BYTEARRAY_SUBSTRING(poolId, 1,20) = t.token
ORDER BY 1 DESC),

joins AS(
SELECT DATE_TRUNC('day', evt_block_time) as block_date, SUM(amountIn/POWER(10,18)) as ajoins
FROM balancer_v2_arbitrum.Vault_evt_Swap
WHERE tokenIn = 0xade4a71bb62bec25154cfc7e6ff49a513b491e81
    AND BYTEARRAY_SUBSTRING(poolId,1,20) = tokenIn
GROUP BY 1),

exits AS(
SELECT DATE_TRUNC('day', evt_block_time) as block_date, SUM(amountOut/POWER(10,18)) as aexits
FROM balancer_v2_arbitrum.Vault_evt_Swap
WHERE tokenOut = 0xade4a71bb62bec25154cfc7e6ff49a513b491e81
    AND BYTEARRAY_SUBSTRING(poolId,1,20) = tokenOut
GROUP BY 1),

joins_and_exits AS(
SELECT j.block_date AS day, SUM(COALESCE(ajoins,0) - COALESCE(aexits,0)) OVER (ORDER BY j.block_date ASC) AS delta
FROM joins j
FULL OUTER JOIN exits e ON j.block_date = e.block_date
)

SELECT
  block_day AS day,
  token_address AS token,
    (SUM(amount_raw)/POWER(10,18) + COALESCE(MAX(delta),0) - COALESCE(CAST(MAX(premints) AS INT256), 0)) AS supply
FROM balances_arbitrum.erc20_day b
LEFT JOIN premints p ON b.token_address = p.token
LEFT JOIN joins_and_exits j ON j.day = b.block_day 
WHERE
  token_address = 0xade4a71bb62bec25154cfc7e6ff49a513b491e81
  AND wallet_address != 0x0000000000000000000000000000000000000000
  AND rn = 1
GROUP BY 1, 2
ORDER BY 1 DESC