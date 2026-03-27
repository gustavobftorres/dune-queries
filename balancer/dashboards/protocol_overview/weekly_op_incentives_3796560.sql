-- part of a query repo
-- query name: Weekly $OP Incentives
-- query link: https://dune.com/queries/3796560


WITH gauges_labels AS (
  SELECT
    child_gauge_address AS address,
    name
  FROM labels.balancer_gauges_optimism
)

SELECT
    date_trunc('week', evt_block_time) AS week,
    name AS gauge_symbol,
    SUM(amount / pow(10, 18)) AS token_amount
FROM balancer_optimism.ChildChainGaugeInjector_evt_EmissionsInjection i
LEFT JOIN  gauges_labels l
ON l.address = i.gauge
GROUP BY 1, 2