-- part of a query repo
-- query name: MEV Capture - Cumulative Priority Fee
-- query link: https://dune.com/queries/5447277


SELECT
    DATE_TRUNC('day', block_time) AS block_date,
    SUM(priority_fee) / 1e9 as daily_priority_fee,
    SUM(SUM(priority_fee)) OVER (ORDER BY DATE_TRUNC('day', block_time)) / 1e9 as cumulative_priority_fee
FROM dune.balancer.result_mev_capture_executed_swaps
GROUP BY 1
ORDER BY 1
