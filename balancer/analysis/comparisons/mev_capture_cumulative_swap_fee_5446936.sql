-- part of a query repo
-- query name: MEV Capture - Cumulative Swap Fee
-- query link: https://dune.com/queries/5446936


SELECT
    DATE_TRUNC('day', block_time) AS block_date,
    SUM(static_swap_fee_usd) as daily_static_swap_fee_usd,
    SUM(dynamic_swap_fee_usd) as daily_dynamic_swap_fee_usd,
    SUM(static_swap_fee_usd + dynamic_swap_fee_usd) as daily_total_swap_fee_usd,
    SUM(SUM(static_swap_fee_usd)) OVER (ORDER BY DATE_TRUNC('day', block_time)) as cumulative_static_swap_fee_usd,
    SUM(SUM(dynamic_swap_fee_usd)) OVER (ORDER BY DATE_TRUNC('day', block_time)) as cumulative_dynamic_swap_fee_usd,
    SUM(SUM(static_swap_fee_usd + dynamic_swap_fee_usd)) OVER (ORDER BY DATE_TRUNC('day', block_time)) as cumulative_total_swap_fee_usd
FROM dune.balancer.result_mev_capture_executed_swaps
GROUP BY 1
ORDER BY 1
