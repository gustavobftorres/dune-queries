-- part of a query repo
-- query name: Pool Creator Fees Collected
-- query link: https://dune.com/queries/4193577


WITH daily_pool_creator_fee_collected AS (
    SELECT
        date_trunc('day', evt_block_time) AS day,
        pool AS pool_id,
        recipient AS pool_creator,
        token AS token_address,
        SUM(amount) AS pool_creator_fee_amount_raw
    FROM balancer_testnet_sepolia.ProtocolFeeController_evt_PoolCreatorFeesWithdrawn b
    GROUP BY 1, 2, 3, 4
),
aggregated_fees AS (
    SELECT 
        pool_creator,
        SUM(pool_creator_fee_amount_raw) AS token_amount_raw
    FROM daily_pool_creator_fee_collected
    GROUP BY pool_creator
)

SELECT 
    ROW_NUMBER() OVER (PARTITION BY a.pool_creator ORDER BY a.token_amount_raw DESC) AS ranking,
    a.pool_creator,
    a.token_amount_raw
FROM aggregated_fees a
ORDER BY ranking DESC
