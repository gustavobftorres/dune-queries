-- part of a query repo
-- query name: Weekly $ARB Incentives (since STIP started)
-- query link: https://dune.com/queries/3223289


WITH gauges_labels AS (
  SELECT
    child_gauge_address AS address,
    name
  FROM labels.balancer_gauges_arbitrum
)

SELECT
    date_trunc('week', evt_block_time) AS week,
    name AS gauge_symbol,
    SUM(amount / pow(10, 18)) AS token_amount
FROM balancer_v2_arbitrum.ChildChainGaugeInjector_evt_EmissionsInjection i
LEFT JOIN gauges_labels l
ON l.address = i.gauge
WHERE token = 0x912ce59144191c1204e64559fe8253a0e49e6548
GROUP BY 1, 2