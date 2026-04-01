-- part of a query repo
-- query name: opt_gauge_mapping
-- query link: https://dune.com/queries/3817247


SELECT distinct
    'optimism' AS blockchain,
    pools.address AS pool_address,
    child.output_0 AS address,
    pools.name AS name
FROM balancer_ethereum.CappedOptimismRootGaugeFactory_call_create call
    LEFT JOIN balancer_optimism.ChildChainGaugeFactory_call_create child ON child.output_0 = call.recipient
    LEFT JOIN labels.balancer_v2_pools_optimism pools ON pools.address = child.pool