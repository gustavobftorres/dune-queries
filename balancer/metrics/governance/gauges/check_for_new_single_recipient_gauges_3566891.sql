-- part of a query repo
-- query name: Check for new Single Recipient Gauges
-- query link: https://dune.com/queries/3566891


SELECT * 
FROM balancer_ethereum.SingleRecipientGaugeFactory_evt_GaugeCreated g
LEFT JOIN balancer.single_recipient_gauges t ON
g.gauge = t.gauge_address
WHERE evt_block_time >= now() - interval '30' day
AND t.project IS NULL

UNION ALL

SELECT
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL