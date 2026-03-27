-- part of a query repo
-- query name: Gas used per swap on ethereum
-- query link: https://dune.com/queries/3093245


    SELECT DISTINCT hash, tx.gas_used, POWER(SUM(CASE WHEN event_name = 'Swap' THEN 1 ELSE 0 END),.5) as n_swap, tx.gas_used/POWER(SUM(CASE WHEN event_name = 'Swap' THEN 1 ELSE 0 END),.5) as gas_tx
    FROM balancer_v2_ethereum.Vault_evt_Swap AS s
    INNER JOIN ethereum.transactions AS tx ON s.evt_tx_hash = tx.hash
    INNER JOIN ethereum.logs_decoded AS lg ON s.evt_tx_hash = lg.tx_hash
    WHERE tx.block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day) 
    AND s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    AND "to" = 0xba12222222228d8ba445958a75a0704d566bf2c8
    GROUP BY 1,2
    ORDER BY 4 ASC