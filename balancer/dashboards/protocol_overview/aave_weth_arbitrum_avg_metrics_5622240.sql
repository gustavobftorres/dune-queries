-- part of a query repo
-- query name: AAVE/WETH Arbitrum Avg. Metrics
-- query link: https://dune.com/queries/5622240


SELECT
    AVG(tvl_usd) as avg_tvl_usd,
    AVG(swap_amount_usd) as avg_volume_usd,
    AVG(0.0025 * swap_amount_usd) as avg_fees_usd,
    100 * AVG(365 * 0.0025 * swap_amount_usd / tvl_usd) as avg_apr
FROM balancer.pools_metrics_daily
WHERE blockchain = 'arbitrum'
AND project_contract_address = 0x5ea58d57952b028c40bd200e5aff20fc4b590f51
AND block_date >= CURRENT_DATE - INTERVAL '7' DAY
AND block_date < CURRENT_DATE
