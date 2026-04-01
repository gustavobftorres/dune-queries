-- part of a query repo
-- query name: bcow_transfers
-- query link: https://dune.com/queries/3887199


    SELECT DISTINCT * FROM (
        SELECT
            transfer.contract_address,
            transfer.evt_tx_hash,
            transfer.evt_index,
            transfer.evt_block_time,
            TRY_CAST(date_trunc('DAY', transfer.evt_block_time) AS date) AS block_date,
            TRY_CAST(date_trunc('MONTH', transfer.evt_block_time) AS date) AS block_month,
            transfer.evt_block_number,
            transfer."from",
            transfer.to,
            transfer.value
        FROM balancer_testnet_sepolia.BcowPool_evt_transfer transfer)