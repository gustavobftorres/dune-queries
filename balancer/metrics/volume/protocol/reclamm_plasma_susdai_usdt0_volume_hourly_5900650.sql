-- part of a query repo
-- query name: reCLAMM Plasma sUSDai/USDT0 Volume Hourly
-- query link: https://dune.com/queries/5900650


WITH swap_increments AS (
    SELECT
        date_trunc('hour', S.evt_block_time) as "hour",
        CASE
            WHEN S.tokenIn = 0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb THEN S.amountIn / 1e6
            ELSE S.amountOut / 1e6
        END as volume_usd
    FROM balancer_v3_multichain.vault_evt_swap S
    WHERE S.pool = 0xb3ca3ead1c59ded552cd30a6992038284b418b65
        AND S.evt_block_time >= TIMESTAMP '2025-09-25 16:00:00' 
        AND S.evt_block_time <= now()
)
SELECT 
    "hour", 
    SUM(volume_usd) as volume_usd
FROM swap_increments
GROUP BY "hour"
