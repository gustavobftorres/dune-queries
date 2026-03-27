-- part of a query repo
-- query name: Linear Pool - BPT supply
-- query link: https://dune.com/queries/3358139


WITH premints AS (SELECT
  block_day AS day,
  token_address AS token,
  (SUM(amount_raw))
  / POWER(10, 18) AS supply
FROM balances_arbitrum.erc20_day
WHERE
  token_address = 0x7c82a23b4c48d796dee36a9ca215b641c6a8709d
  AND wallet_address != 0x0000000000000000000000000000000000000000
GROUP BY 1, 2
ORDER BY 1 DESC),

joins AS(
SELECT DATE_TRUNC('day', evt_block_time) as block_date, SUM(value/POWER(10,18)) as ajoins
FROM test_schema.git_dunesql_bb8a48d7_balancer_v2_arbitrum_transfers_bpt
WHERE contract_address = 0x7c82a23b4c48d796dee36a9ca215b641c6a8709d
AND "from" = 0x0000000000000000000000000000000000000000
GROUP BY 1

UNION ALL

SELECT DATE_TRUNC('day', evt_block_time) as block_date, SUM(amountOut/POWER(10,18)) as ajoins
FROM balancer_v2_arbitrum.Vault_evt_Swap
WHERE tokenOut = 0x7c82a23b4c48d796dee36a9ca215b641c6a8709d
    AND BYTEARRAY_SUBSTRING(poolId,1,20) = tokenOut
GROUP BY 1),

exits AS(
SELECT DATE_TRUNC('day', evt_block_time) as block_date, SUM(value/POWER(10,18)) as aexits
FROM test_schema.git_dunesql_bb8a48d7_balancer_v2_arbitrum_transfers_bpt
WHERE contract_address = 0x7c82a23b4c48d796dee36a9ca215b641c6a8709d
AND "to" = 0x0000000000000000000000000000000000000000
GROUP BY 1

UNION ALL

SELECT DATE_TRUNC('day', evt_block_time) as block_date, SUM(amountIn/POWER(10,18)) as ajoins
FROM balancer_v2_arbitrum.Vault_evt_Swap
WHERE tokenIn = 0x7c82a23b4c48d796dee36a9ca215b641c6a8709d
    AND BYTEARRAY_SUBSTRING(poolId,1,20) = tokenIn
GROUP BY 1
),

joins_and_exits AS(
SELECT j.block_date, SUM(COALESCE(ajoins,0) - COALESCE(aexits,0)) OVER (ORDER BY j.block_date ASC) AS delta
FROM joins j
FULL OUTER JOIN exits e ON j.block_date = e.block_date
)

SELECT p.day, SUM(COALESCE(delta,0) - (CAST('5192296858534827628530496329220095' AS UINT256))/POWER(10,18))
FROM premints p
LEFT JOIN joins_and_exits j ON p.day = j.block_date
GROUP BY 1