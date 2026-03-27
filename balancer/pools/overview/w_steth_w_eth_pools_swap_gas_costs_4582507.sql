-- part of a query repo
-- query name: (w)stETH - (W)ETH Pools Swap Gas Costs
-- query link: https://dune.com/queries/4582507



SELECT 
    DATE_TRUNC('day', tx.block_time) AS block_date, 
    project_contract_address, 
    APPROX_PERCENTILE(tx.gas_used, 0.5) AS median_gas
FROM ethereum.transactions tx
JOIN dex.trades s ON s.tx_hash = tx.hash
AND s.blockchain = 'ethereum'
WHERE project_contract_address IN (0xdc24316b9ae028f1497c275eb9192a3ea0f67022,
0x93d199263632a4ef4bb438f1feb99e57b4b5f0bd,
0x109830a1aaad605bbf02a9dfa7b0b92ec2fb7daa,
0x0b1a513ee24972daef112bc777a5610d4325c9e7,
0xc4ce391d82d164c166df9c8336ddf84206b2f812)
AND s.block_date >= CURRENT_DATE - INTERVAL '40' DAY
GROUP BY 1, 2
