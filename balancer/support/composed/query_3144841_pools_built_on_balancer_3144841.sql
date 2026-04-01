-- part of a query repo
-- query name: (query_3144841) pools_built_on_balancer
-- query link: https://dune.com/queries/3144841


WITH gyro_pools as (
SELECT r.poolId, 'arbitrum' as blockchain, COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) as name , 'gyroscope' as project 
FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered r
INNER JOIN labels.balancer_v2_pools_arbitrum l ON r.poolAddress = l.address AND l.pool_type = 'ECLP'

UNION ALL 

SELECT r.poolId, 'optimism' as blockchain, COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) as name , 'gyroscope' as project 
FROM balancer_v2_optimism.Vault_evt_PoolRegistered r
INNER JOIN labels.balancer_v2_pools_optimism l ON r.poolAddress = l.address AND l.pool_type = 'ECLP'

UNION ALL 

SELECT r.poolId, 'ethereum' as blockchain, COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) as name , 'gyroscope' as project 
FROM balancer_v2_ethereum.Vault_evt_PoolRegistered r
INNER JOIN labels.balancer_v2_pools_ethereum l ON r.poolAddress = l.address AND l.pool_type = 'ECLP'

UNION ALL 

SELECT r.poolId, 'zkevm' as blockchain, COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) as name , 'gyroscope' as project 
FROM balancer_v2_zkevm.Vault_evt_PoolRegistered r
INNER JOIN labels.balancer_v2_pools_zkevm l ON r.poolAddress = l.address AND l.pool_type = 'ECLP'

UNION ALL 

SELECT r.poolId, 'polygon' as blockchain, COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) as name , 'gyroscope' as project 
FROM balancer_v2_polygon.Vault_evt_PoolRegistered r
INNER JOIN labels.balancer_v2_pools_polygon l ON r.poolAddress = l.address AND l.pool_type = 'ECLP'

UNION ALL 

SELECT r.poolId, 'gnosis' as blockchain, COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) as name , 'gyroscope' as project 
FROM balancer_v2_gnosis.Vault_evt_PoolRegistered r
INNER JOIN labels.balancer_v2_pools_gnosis l ON r.poolAddress = l.address AND l.pool_type = 'ECLP'),
    
xave_pools as (
SELECT * FROM (values
(0x726e324c29a1e49309672b244bdc4ff62a270407000200000000000000000702, 'polygon', 'FX USDC/XSGD', 'xave finance'),
(0x216b176513c500dbe1d677939103e350a9373a390002000000000000000008da, 'polygon', 'FX USDC/DAI', 'xave finance'),
(0xfd24afa5416c8de94fdbaf344840f524155a4dd00002000000000000000008db, 'polygon', 'FX USDC/EURS', 'xave finance'),
(0x32cc63ffeccb7c0508d64e4d37145313cc053b27000200000000000000000cb4, 'polygon', 'FX USDC/VCHF', 'xave finance'),
(0x427333b9f9d8bd0b67fd5fc2213371db0ef178e1000200000000000000000cb0, 'polygon', 'FX USDC/BRLA', 'xave finance'),
(0x6bf004bee6346852a29239b386ab4239ffbd66de000200000000000000000cb5, 'polygon', 'FX USDC/VNXAU', 'xave finance'),
(0xe6d8fcd23ed4e417d7e9d1195edf2ca634684e0e000200000000000000000caf, 'polygon', 'FX USDC/XSGD', 'xave finance'),
(0x55bec22f8f6c69137ceaf284d9b441db1b9bfedc0002000000000000000003cd, 'ethereum', 'FX USDC/XSGD', 'xave finance'),
(0x66bb9d104c55861feb3ec3559433f01f6373c9660002000000000000000003cf, 'ethereum', 'FX USDC/DAI', 'xave finance'),
(0xad0e5e0778cac28f1ff459602b31351871b5754a0002000000000000000003ce, 'ethereum', 'FX USDC/EURS', 'xave finance'),
(0x5f8b11995d7f95faa59ca6fd5ffa1c0dbbe0ec7b000200000000000000000630, 'ethereum', 'FX USDC/EURS', 'xave finance'),
(0x73f8e7a9a19e284a9ac85704af58454cfe75f059000200000000000000000631, 'ethereum', 'FX USDC/GBPT', 'xave finance'),
(0xad0e5e0778cac28f1ff459602b31351871b5754a000200000000000000000029, 'avalanche_c', 'FX USDC/EUROC', 'xave finance'),
(0x0099111ed107bdf0b05162356aee433514aac44000020000000000000000002f, 'avalanche_c', 'FX USDC/VCHF', 'xave finance'),
(0x28f3a9e42667519c83cb090b5c4f6bd34e9f5569000200000000000000000031, 'avalanche_c', 'FX USDC/VEUR', 'xave finance'),
(0x7a1a919c033ebc0d9f23cbf2aa41c24aef826ca200020000000000000000002e, 'avalanche_c', 'FX USDC/EUROC', 'xave finance')
)
    as t (pool_id, blockchain, name, project)),
    
fjord_pools as(
SELECT poolId, lower(chain) as blockchain, name, 'fjord' as project
FROM query_2977657
),

beets_pools as(
SELECT r.poolId, 'optimism' as blockchain, COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) as name , 'beethoven x' as project 
FROM balancer_v2_optimism.Vault_evt_PoolRegistered r
LEFT JOIN labels.balancer_v2_pools_optimism l ON r.poolAddress = l.address

UNION ALL 

SELECT r.poolId, 'fantom' as blockchain, COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) as name , 'beethoven x' as project 
FROM beethoven_x_fantom.Vault_evt_PoolRegistered r
LEFT JOIN labels.beethoven_x_pools_fantom l ON r.poolAddress = l.address
),

jly_pools AS(
SELECT r.poolId, 'sei' as blockchain, COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) as name , 'jelly_swap' as project 
FROM jelly_swap_sei.Vault_evt_PoolRegistered r
LEFT JOIN labels.jelly_swap_pools_sei l ON r.poolAddress = l.address
),

cowamm_pools AS(
SELECT DISTINCT pool_id, blockchain, COALESCE(pool_symbol, CAST(BYTEARRAY_SUBSTRING(pool_id,3,8) AS VARCHAR)) as name , 'BCoWAMM' as project 
FROM balancer.liquidity
WHERE pool_type = 'balancer_cowswap_amm'
)

SELECT *, poolId AS pool_id FROM gyro_pools
UNION ALL
SELECT *,  pool_id FROM xave_pools
UNION ALL
SELECT *, poolId AS pool_id FROM fjord_pools
UNION ALL
SELECT *, poolId AS pool_id FROM beets_pools
UNION ALL
SELECT *, poolId AS pool_id FROM jly_pools
UNION ALL
SELECT *, pool_id FROM cowamm_pools
