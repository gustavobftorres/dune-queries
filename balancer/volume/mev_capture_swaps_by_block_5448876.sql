-- part of a query repo
-- query name: MEV Capture - Swaps by Block
-- query link: https://dune.com/queries/5448876


SELECT
    call_block_number AS block_number,
    COUNT(*) AS total_swaps,
    SUM(CASE WHEN call_success = true THEN 1 ELSE 0 END) AS executed_swaps,
    SUM(CASE WHEN call_success = false THEN 1 ELSE 0 END) AS reverted_swaps,
    ROUND(SUM(CASE WHEN call_success = false THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS revert_rate
FROM balancer_v3_base.vault_call_swap
WHERE JSON_EXTRACT_SCALAR(vaultSwapParams, '$.pool') = '0xd0bfa4784285acd49e06e12f302c2441c5923bfd'
GROUP BY 1
ORDER BY 2 DESC
