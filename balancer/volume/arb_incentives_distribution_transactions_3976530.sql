-- part of a query repo
-- query name: Arb  Incentives Distribution - Transactions
-- query link: https://dune.com/queries/3976530


SELECT
    t.block_time AS block_time,
    pool_address,
    t."from" AS gauge_address,
    l.name AS gauge_symbol,
    t."to" AS recipient_address,
    p.symbol,
    t.amount AS token_amount,
    t.amount * price AS tokens_amount_usd,
    CONCAT('<a target="_blank" href="https://optimistic.etherscan.io/tx/', CAST(evt_tx_hash AS VARCHAR), '">', CAST(evt_tx_hash AS VARCHAR), '↗</a>') AS tx_hash
FROM tokens_arbitrum.transfers t
INNER JOIN  balancer_v2_arbitrum.ChildChainGaugeInjector_evt_EmissionsInjection i
ON t."from" = i.gauge
--AND t.contract_address = i.token
LEFT JOIN labels.balancer_gauges_arbitrum l
ON t."from" = l.child_gauge_address
LEFT JOIN prices.usd p
ON t.contract_address = p.contract_address
AND DATE_TRUNC('minute', t.block_time) = p.minute
AND p.blockchain = 'arbitrum'
WHERE evt_block_time <= TIMESTAMP '{{End date}}'
AND evt_block_time >= TIMESTAMP '{{Start date}}'
AND p.symbol IS NOT NULL
ORDER BY 1 DESC
--bpt da gauge -> stake na aura -> voterproxy -> vault token (1:1 BPT) -> compounder