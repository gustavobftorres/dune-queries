-- part of a query repo
-- query name: $ARB Incentives Metrics
-- query link: https://dune.com/queries/3223158


SELECT SUM(amount / POW(10, 18)) AS arb_incentives, COUNT(DISTINCT gauge) AS distinct_gauges
FROM balancer_v2_arbitrum.ChildChainGaugeInjector_evt_EmissionsInjection i
WHERE evt_block_time <= TIMESTAMP '{{End date}}'
AND evt_block_time >= TIMESTAMP '{{Start date}}'
AND token = 0x912ce59144191c1204e64559fe8253a0e49e6548
