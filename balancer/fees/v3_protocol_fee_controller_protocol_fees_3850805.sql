-- part of a query repo
-- query name: v3_protocol_fee_controller_protocol_fees
-- query link: https://dune.com/queries/3850805


WITH daily_protocol_fee_collected AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            pool AS pool_id,
            token AS token_address,
            'swap_fee' AS fee_type,
            SUM(amount) AS protocol_fee_amount_raw
        FROM balancer_testnet_sepolia.ProtocolFeeController_evt_ProtocolSwapFeeCollected b
        GROUP BY 1, 2, 3, 4 

        UNION ALL          

        SELECT
            date_trunc('day', evt_block_time) AS day,
            pool AS pool_id,
            token AS token_address,
            'yield_fee' AS fee_type,
            SUM(amount) AS protocol_fee_amount_raw
        FROM balancer_testnet_sepolia.ProtocolFeeController_evt_ProtocolYieldFeeCollected b
        GROUP BY 1, 2, 3, 4
    )
        SELECT 
            d.day, 
            d.pool_id, 
            d.token_address, 
            d.fee_type,
            SUM(d.protocol_fee_amount_raw) AS token_amount_raw
        FROM daily_protocol_fee_collected d
        GROUP BY 1, 2, 3, 4
    