-- part of a query repo
-- query name: hiddenhand voting markets on arb
-- query link: https://dune.com/queries/3976357


WITH bribs AS(
SELECT
    evt_tx_hash,
    evt_index,
    evt_block_time,
    evt_block_number,
    market,
    CASE WHEN market = 0xa8214b4fb98936ed45463956afd24a862cc86dc1 THEN 'balancer'
        WHEN market = 0x928b06229a3f4bc7806d80fe54e48e777bb74536 THEN 'aura'
    END AS project,
    FROM_UNIXTIME(deadline) AS deadline,
    briber,
    token,
    t.symbol,
    q.proposal AS gauge_address,
    amount AS amount_raw,
    amount / POWER(10, t.decimals) AS amount,
    amount * price / POWER(10, t.decimals) AS amount_usd
FROM hiddenhand_arbitrum.BribeVault_evt_DepositBribe b
LEFT JOIN prices.usd t ON t.contract_address = b.token 
AND t.blockchain = 'arbitrum'
AND DATE_TRUNC('minute',  evt_block_time) = t.minute
LEFT JOIN query_3971574 q ON CAST(b.proposal AS VARCHAR) = q.proposalHash
WHERE market IN (0x928b06229a3f4bc7806d80fe54e48e777bb74536, 0xa8214b4fb98936ed45463956afd24a862cc86dc1)
AND evt_block_time >= TIMESTAMP '2024-06-20 00:00:00'
AND t.symbol = 'ARB')

SELECT b.*, l.address, l.name, l.child_gauge_address AS gauge FROM bribs b
LEFT JOIN labels.balancer_gauges_arbitrum l
ON b.gauge_address = CAST(l.address AS VARCHAR)