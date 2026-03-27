-- part of a query repo
-- query name: Pool creator fees
-- query link: https://dune.com/queries/4184288


WITH daily_pool_creator_fee_collected AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            pool AS pool_id,
            recipient AS pool_creator,
            token AS token_address,
            SUM(amount) AS pool_creator_fee_amount_raw
        FROM balancer_testnet_sepolia.ProtocolFeeController_evt_PoolCreatorFeesWithdrawn b
        GROUP BY 1, 2, 3, 4 
    )
        SELECT 
            d.day, 
            d.pool_id, 
            d.pool_creator,
            d.token_address, 
            SUM(d.pool_creator_fee_amount_raw) AS token_amount_raw
        FROM daily_pool_creator_fee_collected d
        GROUP BY 1, 2, 3, 4
    