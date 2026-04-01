-- part of a query repo
-- query name: Weekly $ARB Incentives STIP 2nd phase
-- query link: https://dune.com/queries/3981256


WITH gauges_labels AS (
  SELECT
    address AS gauge_address,
    child_gauge_address AS address,
    name
  FROM labels.balancer_gauges_arbitrum
)

SELECT
    evt_tx_hash,
    evt_block_time,
    name AS gauge_symbol,
    l.gauge_address,
    SUM(amount / pow(10, 18)) AS arb_amount,
    SUM(amount * price / pow(10, 18)) AS arb_amount_usd
FROM balancer_v2_arbitrum.ChildChainGaugeInjector_evt_EmissionsInjection i
LEFT JOIN  gauges_labels l
ON l.address = i.gauge
LEFT JOIN prices.usd p ON date_trunc('week', evt_block_time) = p.minute
AND i.token = p.contract_address
AND p.blockchain = 'arbitrum'
WHERE token = 0x912ce59144191c1204e64559fe8253a0e49e6548
AND evt_block_time >= TIMESTAMP '2024-06-20 00:00:00'
GROUP BY 1, 2, 3, 4