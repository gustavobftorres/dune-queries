-- part of a query repo
-- query name: BEETS bridged from and to OPTIMISM - TXs
-- query link: https://dune.com/queries/3781645


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
WHERE currency_symbol = 'BEETS'
AND (source_chain_id = 111 OR destination_chain_id = 111) --optimism chain_id on lz
AND user_address NOT IN 
(0x693f30c37d5a0db9258c636e93ccf011acd8c90c) -- signer, bridged initial treasury badge
AND block_time < TIMESTAMP '2024-05-02 00:00' --snapshot