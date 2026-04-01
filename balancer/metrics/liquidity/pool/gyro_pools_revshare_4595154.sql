-- part of a query repo
-- query name: gyro pools revshare
-- query link: https://dune.com/queries/4595154


WITH gyro_pools AS (
   SELECT pool_address, pool_symbol, SUM(pool_liquidity_usd) AS pool_liquidity, blockchain
   FROM balancer.liquidity
   WHERE blockchain != 'zkevm' -- ignored because: it's not relevant, no gyro config decoded
   AND day = CURRENT_DATE
   AND pool_type = 'ECLP'
   GROUP BY 1, 2, 4
),

pool_keys AS (
   SELECT 
       pool_address,
       pool_symbol,
       pool_liquidity,
       blockchain,
       keccak(varbinary_concat(
           RPAD(CAST('PROTOCOL_SWAP_FEE_PERC' AS VARBINARY), 32, 0x00),
           LPAD(CAST(pool_address AS VARBINARY), 32, 0x00)
       )) as fee_percentage_pool_specific_key,
       keccak(varbinary_concat(
           RPAD(CAST('PROTOCOL_FEE_GYRO_PORTION' AS VARBINARY), 32, 0x00),
           LPAD(CAST(pool_address AS VARBINARY), 32, 0x00)
       )) as fee_gyro_share_pool_specific_key,
       keccak(varbinary_concat(
           RPAD(TRY_CAST('PROTOCOL_SWAP_FEE_PERC' AS VARBINARY), 32, 0x00),
           RPAD(TRY_CAST('ECLP' AS VARBINARY), 32, 0x00)
       )) as fee_percentage_pool_type_key,
       keccak(varbinary_concat(
           RPAD(TRY_CAST('PROTOCOL_FEE_GYRO_PORTION' AS VARBINARY), 32, 0x00),
           RPAD(TRY_CAST('ECLP' AS VARBINARY), 32, 0x00)
       )) as fee_gyro_share_pool_type_key,
       RPAD(CAST('BAL_TREASURY' AS VARBINARY), 32, 0x00) as balancer_treasury_key,
       RPAD(TRY_CAST('GYRO_TREASURY' AS VARBINARY), 32, 0x00) as gyro_treasury_key
   FROM gyro_pools
),

latest_configs AS (
   SELECT
       "key",
       newValue_uint256/1e18 as "value",
        evt_block_time,
        evt_block_number,
       blockchain,
       ROW_NUMBER() OVER (PARTITION BY key, blockchain ORDER BY evt_block_number DESC, evt_index DESC) as rn
   FROM (
       SELECT evt_block_number, evt_index, key, newValue_uint256, 'polygon' as blockchain, evt_block_time
       FROM gyroscope_polygon.GyroConfig_evt_ConfigChanged
       UNION ALL
       SELECT evt_block_number, evt_index, key, newValue_uint256, 'ethereum' as blockchain, evt_block_time 
       FROM gyroscope_ethereum.GyroConfig_evt_ConfigChanged
       UNION ALL
       SELECT evt_block_number, evt_index, key, newValue_uint256, 'base' as blockchain, evt_block_time
       FROM gyroscope_base.GyroConfig_evt_ConfigChanged
       UNION ALL 
       SELECT evt_block_number, evt_index, key, newValue_uint256, 'optimism' as blockchain, evt_block_time 
       FROM gyroscope_optimism.GyroConfig_evt_ConfigChanged
       UNION ALL
       SELECT evt_block_number, evt_index, key, newValue_uint256, 'arbitrum' as blockchain, evt_block_time 
       FROM gyroscope_arbitrum.GyroConfig_evt_ConfigChanged
       UNION ALL
       SELECT evt_block_number, evt_index, key, newValue_uint256, 'avalanche_c' as blockchain, evt_block_time 
       FROM gyroscope_avalanche_c.GyroConfig_evt_ConfigChanged
       UNION ALL
       SELECT evt_block_number, evt_index, key, newValue_uint256, 'gnosis' as blockchain, evt_block_time 
       FROM gyroscope_gnosis.GyroConfig_evt_ConfigChanged
   ) all_configs
),

treasury_setting AS(
   SELECT
       "key",
       newValue_binary as treasury_set,
        evt_block_time,
        evt_block_number,
       blockchain,
       ROW_NUMBER() OVER (PARTITION BY key, blockchain ORDER BY evt_block_number DESC, evt_index DESC) as rn
   FROM (
       SELECT evt_block_number, evt_index, key, newValue_binary, 'polygon' as blockchain, evt_block_time
       FROM gyroscope_polygon.GyroConfig_evt_ConfigChanged
       UNION ALL
       SELECT evt_block_number, evt_index, key, newValue_binary, 'ethereum' as blockchain, evt_block_time 
       FROM gyroscope_ethereum.GyroConfig_evt_ConfigChanged
       UNION ALL
       SELECT evt_block_number, evt_index, key, newValue_binary, 'base' as blockchain, evt_block_time
       FROM gyroscope_base.GyroConfig_evt_ConfigChanged
       UNION ALL 
       SELECT evt_block_number, evt_index, key, newValue_binary, 'optimism' as blockchain, evt_block_time 
       FROM gyroscope_optimism.GyroConfig_evt_ConfigChanged
       UNION ALL
       SELECT evt_block_number, evt_index, key, newValue_binary, 'arbitrum' as blockchain, evt_block_time 
       FROM gyroscope_arbitrum.GyroConfig_evt_ConfigChanged
       UNION ALL
       SELECT evt_block_number, evt_index, key, newValue_binary, 'avalanche_c' as blockchain, evt_block_time 
       FROM gyroscope_avalanche_c.GyroConfig_evt_ConfigChanged
       UNION ALL
       SELECT evt_block_number, evt_index, key, newValue_binary, 'gnosis' as blockchain, evt_block_time 
       FROM gyroscope_gnosis.GyroConfig_evt_ConfigChanged
   ) all_configs
   WHERE newValue_binary IS NOT NULL
),

fees_collected AS (
SELECT
    p.pool_address,
    p.blockchain,
    SUM(amount_raw) / POWER(10,18) AS transfered_amount -- all time fees transfered to balancer protocol fee collector
FROM pool_keys p
LEFT JOIN tokens.transfers t
    ON t.blockchain = p.blockchain
    AND p.pool_address = t.contract_address
    AND t."to"= 0xce88686553686DA562CE7Cea497CE749DA109f9F
GROUP BY 1, 2
)

SELECT 
   p.blockchain,
   p.pool_address,
   p.pool_symbol,
   p.pool_liquidity,
   g.address AS gauge_address,
   g.child_gauge_address AS child_gauge_address,
   COALESCE(
       pool_fee."value",
       type_fee."value",
       0
   ) as protocol_fee_percentage,
   CASE 
       WHEN COALESCE(pool_fee."value", type_fee."value", 0) = 0 THEN NULL
       ELSE 1 - COALESCE(pool_gyro."value", type_gyro."value", 0)
   END as balancer_share,
   CASE 
       WHEN COALESCE(pool_fee."value", type_fee."value", 0) = 0 THEN NULL
       ELSE COALESCE(pool_gyro."value", type_gyro."value", 0)
   END as gyro_share,
    transfered_amount AS transfered_to_balancer_collector,
    baltreasury.treasury_set AS bal_treasury_set,
    gyrotreasury.treasury_set AS gyro_treasury_set,
    GREATEST(baltreasury.evt_block_time, gyrotreasury.evt_block_time, pool_fee.evt_block_time) AS update_at_time,
    GREATEST(baltreasury.evt_block_number, gyrotreasury.evt_block_number, pool_fee.evt_block_number) AS update_block_number
FROM pool_keys p
LEFT JOIN latest_configs pool_fee 
   ON pool_fee."key" = p.fee_percentage_pool_specific_key 
   AND pool_fee.blockchain = p.blockchain
   AND pool_fee.rn = 1
LEFT JOIN latest_configs type_fee 
   ON type_fee."key" = p.fee_percentage_pool_type_key 
   AND type_fee.blockchain = p.blockchain
   AND type_fee.rn = 1
LEFT JOIN latest_configs pool_gyro 
   ON pool_gyro."key" = p.fee_gyro_share_pool_specific_key 
   AND pool_gyro.blockchain = p.blockchain
   AND pool_gyro.rn = 1
LEFT JOIN latest_configs type_gyro 
   ON type_gyro."key" = p.fee_gyro_share_pool_type_key 
   AND type_gyro.blockchain = p.blockchain
   AND type_gyro.rn = 1
LEFT JOIN fees_collected t
    ON t.blockchain = p.blockchain
    AND p.pool_address = t.pool_address
LEFT JOIN treasury_setting baltreasury 
   ON baltreasury."key" = p.balancer_treasury_key
   AND baltreasury.blockchain = p.blockchain
   AND baltreasury.rn = 1
LEFT JOIN treasury_setting gyrotreasury 
   ON gyrotreasury."key" = p.gyro_treasury_key
   AND gyrotreasury.blockchain = p.blockchain
   AND gyrotreasury.rn = 1   
LEFT JOIN labels.balancer_gauges g
    ON g.blockchain = p.blockchain
    AND g.pool_address = p.pool_address
    AND g.status = 'active'
ORDER BY p.pool_liquidity DESC