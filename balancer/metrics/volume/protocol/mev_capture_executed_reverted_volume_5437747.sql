-- part of a query repo
-- query name: MEV Capture Executed/Reverted Volume
-- query link: https://dune.com/queries/5437747


WITH swaps_calls AS (
    SELECT
        call_block_date AS block_date,
        CASE 
            WHEN call_success = true THEN 'executed'
            ELSE 'reverted'
        END AS call_success,
        JSON_EXTRACT_SCALAR(vaultSwapParams, '$.tokenIn') AS token_in_address,
        JSON_EXTRACT_SCALAR(vaultSwapParams, '$.tokenOut') AS token_out_address,
        output_amountIn,
        output_amountOut,
        CASE 
            WHEN JSON_EXTRACT_SCALAR(vaultSwapParams, '$.tokenIn') = '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913' 
            THEN output_amountIn / 1e6 
            ELSE output_amountOut / 1e6
        END AS volume_usd
    FROM balancer_v3_base.vault_call_swap
    WHERE JSON_EXTRACT_SCALAR(vaultSwapParams, '$.pool') = '0xd0bfa4784285acd49e06e12f302c2441c5923bfd'
)
SELECT
    block_date,
    call_success,
    COUNT(*) AS n_swaps,
    SUM(volume_usd) AS volume_usd
FROM swaps_calls
GROUP BY 1, 2
ORDER BY 1 DESC
