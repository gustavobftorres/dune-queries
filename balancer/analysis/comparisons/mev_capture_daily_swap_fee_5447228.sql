-- part of a query repo
-- query name: MEV Capture - Daily Swap Fee
-- query link: https://dune.com/queries/5447228


SELECT
    DATE_TRUNC('day', block_time) AS block_date,
    CASE WHEN has_dynamic_fee = true THEN 'Dynamic' ELSE 'Static' END as fee_type,
    SUM(dynamic_swap_fee_usd) as swap_fee_usd
FROM dune.balancer.result_mev_capture_executed_swaps
WHERE block_time >= timestamp '2025-04-28'
AND block_time <= timestamp '2025-06-14'
GROUP BY 1, 2
ORDER BY 1, 2
