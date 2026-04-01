-- part of a query repo
-- query name: reCLAMM Plasma XPL/USDT0 Volume Daily
-- query link: https://dune.com/queries/5900765


WITH swap_increments AS (
    SELECT
        date_trunc('hour', S.evt_block_time) as "hour",
        CASE
            WHEN S.tokenIn = 0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb THEN S.amountIn / 1e6
            ELSE S.amountOut / 1e6
        END as volume_usd
    FROM balancer_v3_multichain.vault_evt_swap S
    WHERE S.pool = {{pool}}
        AND S.evt_block_time >= TIMESTAMP '2025-09-25 16:00:00' 
        AND S.evt_block_time < now()
)
SELECT 
    "hour", 
    SUM(volume_usd) as volume_usd
FROM swap_increments
GROUP BY "hour"
