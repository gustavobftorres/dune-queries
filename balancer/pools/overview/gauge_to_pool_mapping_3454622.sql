-- part of a query repo
-- query name: Gauge to Pool Mapping
-- query link: https://dune.com/queries/3454622


WITH arb_gauges AS(
WITH reward_gauges AS(
SELECT distinct
    'arbitrum' AS blockchain,
    pools.address AS pool_address,
    gauge.gauge AS gauge_address,
    'arb:' || pools.name AS gauge_name
FROM
    balancer_ethereum.ArbitrumRootGaugeFactory_evt_ArbitrumRootGaugeCreated gauge
    LEFT JOIN balancer_v2_arbitrum.ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated streamer ON gauge.recipient = streamer.streamer
    LEFT JOIN labels.balancer_v2_pools_arbitrum pools ON pools.address = streamer.pool
WHERE pools.name IS NOT NULL

UNION ALL

SELECT distinct
    'arbitrum' AS blockchain,
    pools.address AS pool_address,
    gauge.gauge AS gauge_address,
    'arb:' || pools.name AS gauge_name
FROM
    balancer_ethereum.CappedArbitrumRootGaugeFactory_evt_GaugeCreated gauge
    INNER JOIN balancer_ethereum.CappedArbitrumRootGaugeFactory_call_create call ON call.call_tx_hash = gauge.evt_tx_hash
    LEFT JOIN balancer_v2_arbitrum.ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated streamer ON streamer.streamer = call.recipient
    LEFT JOIN labels.balancer_v2_pools_arbitrum pools ON pools.address = streamer.pool
WHERE pools.name IS NOT NULL),

child_gauges AS(
SELECT distinct
    'arbitrum' AS blockchain,
    pools.address AS pool_address,
    call.output_0 AS gauge_address,
    'arb:' || pools.name AS gauge_name
FROM balancer_ethereum.CappedArbitrumRootGaugeFactory_call_create call
    LEFT JOIN balancer_arbitrum.ChildChainGaugeFactory_call_create child ON child.output_0 = call.recipient
    LEFT JOIN labels.balancer_v2_pools_arbitrum pools ON pools.address = child.pool)

SELECT * FROM reward_gauges
WHERE gauge_name IS NOT NULL
UNION ALL
SELECT * FROM child_gauges
WHERE gauge_name IS NOT NULL),

opt_gauges AS(
WITH reward_gauges AS(
SELECT distinct
    'optimism' AS blockchain,
    pools.address AS pool_address,
    gauge.gauge AS gauge_address,
    'opt:' || pools.name AS gauge_name
FROM
    balancer_ethereum.optimismRootGaugeFactory_evt_optimismRootGaugeCreated gauge
    LEFT JOIN balancer_v2_optimism.ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated streamer ON gauge.recipient = streamer.streamer
    LEFT JOIN labels.balancer_v2_pools_optimism pools ON pools.address = streamer.pool
WHERE pools.name IS NOT NULL

UNION ALL

SELECT distinct
    'optimism' AS blockchain,
    pools.address AS pool_address,
    gauge.gauge AS gauge_address,
    'opt:' || pools.name AS gauge_name
FROM
    balancer_ethereum.CappedoptimismRootGaugeFactory_evt_GaugeCreated gauge
    INNER JOIN balancer_ethereum.CappedoptimismRootGaugeFactory_call_create call ON call.call_tx_hash = gauge.evt_tx_hash
    LEFT JOIN balancer_v2_optimism.ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated streamer ON streamer.streamer = call.recipient
    LEFT JOIN labels.balancer_v2_pools_optimism pools ON pools.address = streamer.pool
WHERE pools.name IS NOT NULL),

child_gauges AS(
SELECT distinct
    'optimism' AS blockchain,
    pools.address AS pool_address,
    call.output_0 AS gauge_address,
    'opt:' || pools.name AS gauge_name
FROM balancer_ethereum.CappedoptimismRootGaugeFactory_call_create call
    LEFT JOIN balancer_optimism.ChildChainGaugeFactory_call_create child ON child.output_0 = call.recipient
    LEFT JOIN labels.balancer_v2_pools_optimism pools ON pools.address = child.pool)

SELECT * FROM reward_gauges
WHERE gauge_name IS NOT NULL
UNION ALL
SELECT * FROM child_gauges
WHERE gauge_name IS NOT NULL),

pol_gauges AS(
WITH reward_gauges AS(
SELECT distinct
    'polygon' AS blockchain,
    pools.address AS pool_address,
    gauge.gauge AS gauge_address,
    'pol:' || pools.name AS gauge_name
FROM
    balancer_ethereum.polygonRootGaugeFactory_evt_polygonRootGaugeCreated gauge
    LEFT JOIN balancer_polygon.ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated streamer ON gauge.recipient = streamer.streamer
    LEFT JOIN labels.balancer_v2_pools_polygon pools ON pools.address = streamer.pool
WHERE pools.name IS NOT NULL

UNION ALL

SELECT distinct
    'polygon' AS blockchain,
    pools.address AS pool_address,
    gauge.gauge AS gauge_address,
    'pol:' || pools.name AS gauge_name
FROM
    balancer_ethereum.CappedpolygonRootGaugeFactory_evt_GaugeCreated gauge
    INNER JOIN balancer_ethereum.CappedpolygonRootGaugeFactory_call_create call ON call.call_tx_hash = gauge.evt_tx_hash
    LEFT JOIN balancer_polygon.ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated streamer ON streamer.streamer = call.recipient
    LEFT JOIN labels.balancer_v2_pools_polygon pools ON pools.address = streamer.pool
WHERE pools.name IS NOT NULL),

child_gauges AS(
SELECT distinct
    'polygon' AS blockchain,
    pools.address AS pool_address,
    call.output_0 AS gauge_address,
    'pol:' || pools.name AS gauge_name
FROM balancer_ethereum.CappedpolygonRootGaugeFactory_call_create call
    LEFT JOIN balancer_polygon.ChildChainGaugeFactory_call_create child ON child.output_0 = call.recipient
    LEFT JOIN labels.balancer_v2_pools_polygon pools ON pools.address = child.pool)

SELECT * FROM reward_gauges
WHERE gauge_name IS NOT NULL
UNION ALL
SELECT * FROM child_gauges
WHERE gauge_name IS NOT NULL),

ava_gauges AS(
SELECT distinct
    'avalanche_c' AS blockchain,
    pools.address AS pool_address,
    call.output_0 AS gauge_address,
    'ava:' || pools.name AS gauge_name
FROM balancer_ethereum.avalancheRootGaugeFactory_call_create call
    LEFT JOIN balancer_avalanche_c.ChildChainGaugeFactory_call_create child ON child.output_0 = call.recipient
    LEFT JOIN labels.balancer_v2_pools_avalanche_c pools ON pools.address = child.pool),
    
    base_gauges AS(
SELECT distinct
    'base' AS blockchain,
    pools.address AS pool_address,
    call.output_0 AS gauge_address,
    'base:' || pools.name AS gauge_name
FROM balancer_ethereum.BaseRootGaugeFactory_call_create call
    LEFT JOIN balancer_base.ChildChainGaugeFactory_call_create child ON child.output_0 = call.recipient
    LEFT JOIN labels.balancer_v2_pools_base pools ON pools.address = child.pool),
    
    gno_gauges AS(
SELECT distinct
    'gnosis' AS blockchain,
    pools.address AS pool_address,
    call.output_0 AS gauge_address,
    'gno:' || pools.name AS gauge_name
FROM balancer_ethereum.GnosisRootGaugeFactory_call_create call
    LEFT JOIN balancer_gnosis.ChildChainGaugeFactory_call_create child ON child.output_0 = call.recipient
    LEFT JOIN labels.balancer_v2_pools_gnosis pools ON pools.address = child.pool),

zkevm_gauges AS(
SELECT distinct
    'zkevm' AS blockchain,
    pools.address AS pool_address,
    call.output_0 AS gauge_address,
    'zkevm:' || pools.name AS gauge_name
FROM balancer_ethereum.PolygonZkEVMRootGaugeFactory_call_create call
    LEFT JOIN balancer_zkevm.ChildChainGaugeFactory_call_create child ON child.output_0 = call.recipient
    LEFT JOIN labels.balancer_v2_pools pools ON pools.address = child.pool
    AND blockchain = 'zkevm'),
    
eth_gauges AS(
SELECT
    'ethereum' AS blockchain,
    pools.address AS pool_address,
    gauge AS gauge_address,
    'eth:' || pools.name AS gauge_name
FROM
    balancer_ethereum.LiquidityGaugeFactory_evt_GaugeCreated gauge
    LEFT JOIN labels.balancer_v2_pools_ethereum pools ON pools.address = gauge.pool
UNION ALL
SELECT
    'ethereum' AS blockchain,
    pools.address AS pool_address,
    gauge AS gauge_address,
    'eth:' || pools.name AS gauge_name
FROM
    balancer_ethereum.CappedLiquidityGaugeFactory_evt_GaugeCreated evt
    INNER JOIN balancer_ethereum.CappedLiquidityGaugeFactory_call_create call ON call.call_tx_hash = evt.evt_tx_hash
    LEFT JOIN labels.balancer_v2_pools_ethereum pools ON pools.address = call.pool
)    
    
SELECT * FROM arb_gauges
UNION ALL
SELECT * FROM ava_gauges
UNION ALL
SELECT * FROM base_gauges
UNION ALL
SELECT * FROM eth_gauges
UNION ALL
SELECT * FROM gno_gauges
UNION ALL
SELECT * FROM opt_gauges
UNION ALL
SELECT * FROM pol_gauges
UNION ALL
SELECT * FROM zkevm_gauges
UNION ALL
SELECT blockchain, BYTEARRAY_SUBSTRING(pool_id,1,20), gauge_address, project FROM balancer.single_recipient_gauges