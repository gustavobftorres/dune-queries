-- part of a query repo
-- query name: transfers_bpt (Dune SQL)
-- query link: https://dune.com/queries/2829894


WITH registered_pools AS (
    SELECT
      DISTINCT poolAddress AS pool_address
    FROM 
        balancer_v2_ethereum.Vault_evt_PoolRegistered
  )
  
     SELECT DISTINCT * FROM (
        SELECT
            logs.contract_address,
            logs.tx_hash AS evt_tx_hash,
            logs.index AS evt_index,
            logs.block_time AS evt_block_time,
            TRY_CAST(date_trunc('DAY', logs.block_time) AS date) AS evt_block_date,
            logs.block_number AS evt_block_number,
            bytearray_substring(topic1, 13) AS "from",
            bytearray_substring(topic2, 13) AS to,
            bytearray_to_uint256(logs.data) AS value
        FROM ethereum.logs logs
        INNER JOIN registered_pools p ON p.pool_address = logs.contract_address
        WHERE logs.topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
            AND logs.block_time >= CAST('2021-04-20' AS TIMESTAMP)
        )
