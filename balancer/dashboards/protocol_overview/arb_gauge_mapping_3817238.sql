-- part of a query repo
-- query name: arb_gauge_mapping
-- query link: https://dune.com/queries/3817238


SELECT distinct
    'arbitrum' AS blockchain,
    pools.address AS pool_address,
    child.output_0 AS address,
    pools.name AS name
FROM balancer_ethereum.CappedArbitrumRootGaugeFactory_call_create call
    LEFT JOIN balancer_arbitrum.ChildChainGaugeFactory_call_create child ON child.output_0 = call.recipient
    LEFT JOIN labels.balancer_v2_pools_arbitrum pools ON pools.address = child.pool