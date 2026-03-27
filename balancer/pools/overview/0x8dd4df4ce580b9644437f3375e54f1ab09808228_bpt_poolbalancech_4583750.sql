-- part of a query repo
-- query name: 0x8dd4df4ce580b9644437f3375e54f1ab09808228 BPT poolBalanceChanged
-- query link: https://dune.com/queries/4583750


        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            token AS token_address,
            SUM(protocol_fees) AS protocol_fee_amount_raw
        FROM balancer_v2_gnosis.Vault_evt_PoolBalanceChanged b
        CROSS JOIN unnest("protocolFeeAmounts", "tokens") AS t(protocol_fees, token)
        WHERE token = 0x8dd4df4ce580b9644437f3375e54f1ab09808228
        GROUP BY 1, 2, 3 