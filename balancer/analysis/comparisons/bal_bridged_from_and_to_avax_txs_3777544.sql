-- part of a query repo
-- query name: BAL bridged from and to AVAX - TXs
-- query link: https://dune.com/queries/3777544


SELECT 
    blockchain,
    source_chain_id,
    tx_hash,
    block_number,
    block_time,
    user_address,
    currency_symbol,
    currency_contract,
    amount_usd,
    amount_original,
    amount_raw
FROM layerzero.send
WHERE currency_symbol = 'BAL'
AND (source_chain_id = 106 OR destination_chain_id = 106) --avax chain_id on lz
AND user_address NOT IN 
(0xe9735f7d85a57bfb860c1e2c1c7b4f587ba0f6e7,
0xfb7d0d001bc8d0bc998071c762bff53ee31b725f) --checkpointer and gauges removal
AND block_time < TIMESTAMP '2024-05-02 00:00' --snapshot