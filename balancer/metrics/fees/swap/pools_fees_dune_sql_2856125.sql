-- part of a query repo
-- query name: pools_fees (Dune SQL)
-- query link: https://dune.com/queries/2856125





WITH registered_pools AS (
    SELECT
      DISTINCT poolAddress AS pool_address
    FROM 
        balancer_v2_polygon.Vault_evt_PoolRegistered
    --WHERE evt_block_time >= DATE_TRUNC('day', NOW() - interval '7' day)
  )
SELECT
    logs.contract_address,
    logs.tx_hash,
    logs.tx_index,
    logs.index,
    logs.block_time,
    logs.block_number,
    CAST(bytearray_to_uint256(bytearray_ltrim(logs.data)) AS DOUBLE) AS swap_fee_percentage
FROM polygon.logs logs
    INNER JOIN registered_pools ON registered_pools.pool_address = logs.contract_address
WHERE logs.topic0 = 0xa9ba3ffe0b6c366b81232caab38605a0699ad5398d6cce76f91ee809e322dafc

    AND logs.block_time >= CAST('2021-04-20' AS TIMESTAMP)

    --AND logs.block_time >= DATE_TRUNC('day', NOW() - interval '1 week')
