-- part of a query repo
-- query name: MEV Capture - Swap Traces
-- query link: https://dune.com/queries/5437700


SELECT
    call_block_time AS block_time,
    call_block_number AS block_number,
    CASE 
        WHEN JSON_EXTRACT_SCALAR(vaultSwapParams, '$.tokenIn') = '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913' 
        THEN 'USDC' 
        ELSE 'WETH' 
    END AS token_bought,
    CASE 
        WHEN JSON_EXTRACT_SCALAR(vaultSwapParams, '$.tokenOut') = '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913' 
        THEN 'USDC' 
        ELSE 'WETH' 
    END AS token_sold,
    output_amountIn AS token_bought_amount,
    output_amountOut AS token_sold_amount,
    call_tx_hash AS tx_hash,
    call_tx_from AS tx_from,
    call_tx_to AS tx_to,
    CASE 
        WHEN call_success = true THEN 'executed'
        ELSE 'reverted'
    END AS call_success,
    TRY(tx_fee_breakdown['base_fee']) * 1e9  AS base_fee,
    TRY(tx_fee_breakdown['priority_fee']) * 1e9  AS priority_fee
FROM balancer_v3_base.vault_call_swap t
JOIN gas_base.fees g
ON g.tx_hash = t.call_tx_hash
WHERE JSON_EXTRACT_SCALAR(vaultSwapParams, '$.pool') = '0xd0bfa4784285acd49e06e12f302c2441c5923bfd'
ORDER BY 1 DESC
