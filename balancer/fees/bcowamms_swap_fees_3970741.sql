-- part of a query repo
-- query name: bcowamms swap fees
-- query link: https://dune.com/queries/3970741


SELECT 'ethereum' AS blockchain, contract_address, call_tx_hash, call_block_time, call_block_number, swapFee / POWER (10,18) AS swapFee FROM b_cow_amm_ethereum.BCoWPool_call_setSwapFee
WHERE call_success
UNION
SELECT 'gnosis' AS blockchain, contract_address, call_tx_hash, call_block_time, call_block_number, swapFee / POWER (10,18) FROM b_cow_amm_gnosis.BCoWPool_call_setSwapFee
WHERE call_success