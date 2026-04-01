-- part of a query repo
-- query name: Gyro Pools
-- query link: https://dune.com/queries/3299252


SELECT DISTINCT pr.poolId as pool_id, cc.name, 'arbitrum' as blockchain
FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered pr
INNER JOIN gyroscope_arbitrum.GyroECLPPoolFactory_call_create cc ON cc.output_0 = pr.poolAddress


UNION ALL

SELECT DISTINCT pr.poolId as pool_id, cc.name, 'arbitrum' as blockchain
FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered pr
INNER JOIN gyroscope_arbitrum.Gyro2CLPPoolFactory_call_create cc ON cc.output_0 = pr.poolAddress

UNION ALL

SELECT DISTINCT pr.poolId as pool_id, cc.name, 'ethereum' as blockchain
FROM balancer_v2_ethereum.Vault_evt_PoolRegistered pr
INNER JOIN gyroscope_ethereum.GyroECLPPoolFactory_call_create cc ON cc.output_0 = pr.poolAddress

UNION ALL

SELECT DISTINCT pr.poolId as pool_id, cc.name, 'optimism' as blockchain
FROM balancer_v2_optimism.Vault_evt_PoolRegistered pr
INNER JOIN gyroscope_optimism.GyroECLPPoolFactory_call_create cc ON cc.output_0 = pr.poolAddress

UNION ALL

SELECT DISTINCT pr.poolId as pool_id, cc.name, 'polygon' as blockchain
FROM balancer_v2_polygon.Vault_evt_PoolRegistered pr
INNER JOIN gyroscope_polygon.GyroECLPPoolFactory_call_create cc ON cc.output_0 = pr.poolAddress

UNION ALL

SELECT DISTINCT pr.poolId as pool_id, cc.name, 'zkevm' as blockchain
FROM balancer_v2_zkevm.Vault_evt_PoolRegistered pr
INNER JOIN gyroscope_zkevm.GyroECLPPoolFactory_call_create cc ON cc.output_0 = pr.poolAddress