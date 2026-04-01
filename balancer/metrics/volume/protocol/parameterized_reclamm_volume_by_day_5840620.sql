-- part of a query repo
-- query name: Parameterized reCLAMM Volume by day
-- query link: https://dune.com/queries/5840620


SELECT 
    date_trunc('day', T.block_time) as "day",
    SUM(T.amount_usd) as "USD Volume"
FROM balancer_v3_multichain.vault_evt_swap S
JOIN dex.trades T ON T.block_time > TIMESTAMP '{{start}}' 
    AND T.project = 'balancer' 
    AND T.blockchain = S.chain 
    AND S.evt_tx_hash = T.tx_hash 
    AND T.evt_index = S.evt_index
WHERE S.chain = '{{chain}}' AND S.pool = {{pool}}
GROUP BY date_trunc('day', T.block_time)