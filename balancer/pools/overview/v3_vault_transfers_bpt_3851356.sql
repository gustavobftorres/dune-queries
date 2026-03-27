-- part of a query repo
-- query name: v3_vault_transfers_bpt
-- query link: https://dune.com/queries/3851356


    WITH registered_pools AS (
        SELECT
        DISTINCT pool AS pool_address
        FROM
        balancer_testnet_sepolia.Vault_evt_PoolRegistered
    )

    SELECT DISTINCT * FROM (
        SELECT
            'sepolia' AS blockchain,
            '3' AS version,
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
        FROM erc20_sepolia.evt_transfer transfer
        INNER JOIN registered_pools p ON p.pool_address = transfer.contract_address) transfers
