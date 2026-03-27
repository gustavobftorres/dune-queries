-- part of a query repo
-- query name: AURA emissions to balancer gauges
-- query link: https://dune.com/queries/3981894


SELECT 
    d.evt_tx_hash,
    d.evt_block_time,
    d.evt_block_number,
    d.evt_index,
    b.lpToken AS pool_address,
    l.name AS pool_symbol,
    l.address AS gauge_address,
    b.gauge AS child_gauge_address,
    d.token,
    p.symbol,
    (d.amount/POWER(10,p.decimals)) AS amount,
    ((d.amount * price)/POWER(10, p.decimals)) AS amount_usd
FROM aura_finance_arbitrum.ChildStashRewardDistro_evt_Queued d
LEFT JOIN aura_finance_arbitrum.BoosterLite_evt_PoolAdded b ON d.pId = b.pid
LEFT JOIN prices.usd p ON d.token = p.contract_address
AND p.blockchain = 'arbitrum'
AND DATE_TRUNC('minute', d.evt_block_time) = p.minute
LEFT JOIN labels.balancer_gauges_arbitrum l ON l.pool_address = b.lpToken
WHERE d.evt_block_time >= TIMESTAMP '2024-06-20 00:00:00'
AND d.token = 0x1509706a6c66ca549ff0cb464de88231ddbe213b