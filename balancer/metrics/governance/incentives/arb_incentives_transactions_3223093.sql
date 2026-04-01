-- part of a query repo
-- query name: $ARB Incentives Transactions
-- query link: https://dune.com/queries/3223093


WITH gauges_labels AS (
  SELECT
    child_gauge_address AS address,
    name
  FROM labels.balancer_gauges_arbitrum
)

SELECT
    evt_block_time AS block_time,
    gauge AS gauge_address,
    name AS gauge_symbol,
    amount / pow(10, 18) AS token_amount,
    CONCAT('<a target="_blank" href="https://arbiscan.io/tx/', CAST(evt_tx_hash AS VARCHAR), '">', CAST(evt_tx_hash AS VARCHAR), '↗</a>') AS tx_hash
FROM balancer_v2_arbitrum.ChildChainGaugeInjector_evt_EmissionsInjection i
LEFT JOIN  gauges_labels l
ON l.address = i.gauge
WHERE evt_block_time <= TIMESTAMP '{{End date}}'
AND evt_block_time >= TIMESTAMP '{{Start date}}'
AND token = 0x912ce59144191c1204e64559fe8253a0e49e6548
ORDER BY 1 DESC
