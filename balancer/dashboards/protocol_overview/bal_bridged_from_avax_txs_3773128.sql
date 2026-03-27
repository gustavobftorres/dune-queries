-- part of a query repo
-- query name: BAL bridged from AVAX - Txs.
-- query link: https://dune.com/queries/3773128


SELECT l.block_date, l.block_time, l.block_number, l.block_hash, l.tx_hash, l.tx_from, t.value / POW(10,18) AS bal_bridged
FROM avalanche_c.logs l
LEFT JOIN erc20_avalanche_c.evt_transfer t ON l.tx_hash = t.evt_tx_hash
WHERE l.contract_address = 0xE15bCB9E0EA69e6aB9FA080c4c4A5632896298C3  --BAL on AVAX
AND l.topic0 = 0xd81fc9b8523134ed613870ed029d6170cbb73aa6a6bc311b9a642689fb9df59a --LZ interaction SendToChain
AND t.to = 0x0000000000000000000000000000000000000000