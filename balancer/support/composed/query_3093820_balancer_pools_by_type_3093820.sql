-- part of a query repo
-- query name: (query_3093820) balancer_pools_by_type
-- query link: https://dune.com/queries/3093820


WITH arb_pools as(
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'Linear' as pool_type
FROM balancer_v2_arbitrum.AaveLinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'Comp. Stable' as pool_type
FROM balancer_v2_arbitrum.ComposableStablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'Linear' as pool_type
FROM balancer_v2_arbitrum.ERC4626LinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'IP' as pool_type
FROM balancer_v2_arbitrum.InvestmentPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'LBP' as pool_type
FROM balancer_v2_arbitrum.LiquidityBootstrappingPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'Managed' as pool_type
FROM balancer_v2_arbitrum.ManagedPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'Stable' as pool_type
FROM balancer_v2_arbitrum.MetaStablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'LBP' as pool_type
FROM balancer_v2_arbitrum.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'Stable' as pool_type
FROM balancer_v2_arbitrum.StablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'Weighted' as pool_type
FROM balancer_v2_arbitrum.WeightedPool2TokensFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'Weighted' as pool_type
FROM balancer_v2_arbitrum.WeightedPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'Weighted' as pool_type
FROM balancer_v2_arbitrum.WeightedPoolV2Factory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'arbitrum' as blockchain, 'Linear' as pool_type
FROM balancer_v2_arbitrum.YearnLinearPoolFactory_evt_PoolCreated
),

av_pools as (
SELECT DISTINCT pool, 'avalanche_c' as blockchain, 'Linear' as pool_type
FROM balancer_v2_avalanche_c.AaveLinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'avalanche_c' as blockchain, 'Comp. Stable' as pool_type
FROM balancer_v2_avalanche_c.ComposableStablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'avalanche_c' as blockchain, 'Linear' as pool_type
FROM balancer_v2_avalanche_c.ERC4626LinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'avalanche_c' as blockchain, 'Managed' as pool_type
FROM balancer_v2_avalanche_c.ManagedPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'avalanche_c' as blockchain, 'LBP' as pool_type
FROM balancer_v2_avalanche_c.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'avalanche_c' as blockchain, 'Weighted' as pool_type
FROM balancer_v2_avalanche_c.WeightedPoolFactory_evt_PoolCreated
),

base_pools as (
SELECT DISTINCT pool, 'base' as blockchain, 'Linear' as pool_type
FROM balancer_v2_base.AaveLinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'base' as blockchain, 'Comp. Stable' as pool_type
FROM balancer_v2_base.ComposableStablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'base' as blockchain, 'Linear' as pool_type
FROM balancer_v2_base.ERC4626LinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'base' as blockchain, 'Linear' as pool_type
FROM balancer_v2_base.GearboxLinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'base' as blockchain, 'Managed' as pool_type
FROM balancer_v2_base.ManagedPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'base' as blockchain, 'LBP' as pool_type
FROM balancer_v2_base.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'base' as blockchain, 'Weighted' as pool_type
FROM balancer_v2_base.WeightedPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'base' as blockchain, 'Linear' as pool_type
FROM balancer_v2_base.YearnLinearPoolFactory_evt_PoolCreated
),

eth_pools as(
SELECT DISTINCT pool, 'ethereum' as blockchain, 'Linear' as pool_type
FROM balancer_v2_ethereum.AaveLinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'Comp. Stable' as pool_type
FROM balancer_v2_ethereum.ComposableStablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'Linear' as pool_type
FROM balancer_v2_ethereum.ERC4626LinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'Linear' as pool_type
FROM balancer_v2_ethereum.EulerLinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'Linear' as pool_type
FROM balancer_v2_ethereum.GearboxLinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'IP' as pool_type
FROM balancer_v2_ethereum.InvestmentPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'LBP' as pool_type
FROM balancer_v2_ethereum.LiquidityBootstrappingPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'Managed' as pool_type
FROM balancer_v2_ethereum.ManagedPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'Stable' as pool_type
FROM balancer_v2_ethereum.MetaStablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'LBP' as pool_type
FROM balancer_v2_ethereum.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'Linear' as pool_type
FROM balancer_v2_ethereum.SiloLinearPoolFactory_evt_PoolCreated
UNION ALL
SELECT DISTINCT pool, 'ethereum' as blockchain, 'Stable' as pool_type
FROM balancer_v2_ethereum.StablePhantomPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'Stable' as pool_type
FROM balancer_v2_ethereum.StablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'Weighted' as pool_type
FROM balancer_v2_ethereum.WeightedPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'ethereum' as blockchain, 'Linear' as pool_type
FROM balancer_v2_ethereum.YearnLinearPoolFactory_evt_PoolCreated
),

gno_pools as(
SELECT DISTINCT pool, 'gnosis' as blockchain, 'Linear' as pool_type
FROM balancer_v2_gnosis.AaveLinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'gnosis' as blockchain, 'Comp. Stable' as pool_type
FROM balancer_v2_gnosis.ComposableStablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'gnosis' as blockchain, 'Linear' as pool_type
FROM balancer_v2_gnosis.ERC4626LinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'gnosis' as blockchain, 'LBP' as pool_type
FROM balancer_v2_gnosis.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
UNION ALL
SELECT DISTINCT pool, 'gnosis' as blockchain, 'Linear' as pool_type
FROM balancer_v2_gnosis.UnbuttonAaveLinearPoolFactory_evt_PoolCreated
),

opt_pools as(
SELECT DISTINCT pool, 'optimism' as blockchain, 'Linear' as pool_type
FROM balancer_v2_optimism.AaveLinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'optimism' as blockchain, 'Comp. Stable' as pool_type
FROM balancer_v2_optimism.ComposableStablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'optimism' as blockchain, 'Linear' as pool_type
FROM balancer_v2_optimism.ERC4626LinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'optimism' as blockchain, 'Managed' as pool_type
FROM balancer_v2_optimism.ManagedPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'optimism' as blockchain, 'Stable' as pool_type
FROM balancer_v2_optimism.MetaStablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'optimism' as blockchain, 'LBP' as pool_type
FROM balancer_v2_optimism.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'optimism' as blockchain, 'Stable' as pool_type
FROM balancer_v2_optimism.StablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'optimism' as blockchain, 'Weighted' as pool_type
FROM balancer_v2_optimism.WeightedPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'optimism' as blockchain, 'Linear' as pool_type
FROM balancer_v2_optimism.YearnLinearPoolFactory_evt_PoolCreated
),

pol_pools as(
SELECT DISTINCT pool, 'polygon' as blockchain, 'Linear' as pool_type
FROM balancer_v2_polygon.AaveLinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'polygon' as blockchain, 'Comp. Stable' as pool_type
FROM balancer_v2_polygon.ComposableStablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'polygon' as blockchain, 'Linear' as pool_type
FROM balancer_v2_polygon.ERC4626LinearPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'polygon' as blockchain, 'IP' as pool_type
FROM balancer_v2_polygon.InvestmentPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'polygon' as blockchain, 'LBP' as pool_type
FROM balancer_v2_polygon.LiquidityBootstrappingPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'polygon' as blockchain, 'Managed' as pool_type
FROM balancer_v2_polygon.ManagedPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'polygon' as blockchain, 'Stable' as pool_type
FROM balancer_v2_polygon.MetaStablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'polygon' as blockchain, 'LBP' as pool_type
FROM balancer_v2_polygon.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'polygon' as blockchain, 'Stable' as pool_type
FROM balancer_v2_polygon.StablePhantomPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'polygon' as blockchain, 'Stable' as pool_type
FROM balancer_v2_polygon.StablePoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'polygon' as blockchain, 'Weighted' as pool_type
FROM balancer_v2_polygon.WeightedPoolFactory_evt_PoolCreated
UNION ALL 
SELECT DISTINCT pool, 'polygon' as blockchain, 'Linear' as pool_type
FROM balancer_v2_polygon.YearnLinearPoolFactory_evt_PoolCreated
)


SELECT * FROM arb_pools
UNION ALL
SELECT * FROM av_pools
UNION ALL
SELECT * FROM base_pools
UNION ALL
SELECT * FROM eth_pools
UNION ALL
SELECT * FROM gno_pools
UNION ALL
SELECT * FROM opt_pools
UNION ALL
SELECT * FROM pol_pools