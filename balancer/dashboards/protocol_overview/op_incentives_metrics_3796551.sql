-- part of a query repo
-- query name: $OP Incentives Metrics
-- query link: https://dune.com/queries/3796551


SELECT SUM(amount / POW(10, 18)) AS op_incentives, COUNT(DISTINCT gauge) AS distinct_gauges
FROM balancer_optimism.ChildChainGaugeInjector_evt_EmissionsInjection i
WHERE evt_block_time <= TIMESTAMP '{{End date}}'
AND evt_block_time >= TIMESTAMP '{{Start date}}'