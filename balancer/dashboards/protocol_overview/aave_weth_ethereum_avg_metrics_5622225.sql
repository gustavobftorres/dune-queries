-- part of a query repo
-- query name: AAVE/WETH Ethereum Avg. Metrics
-- query link: https://dune.com/queries/5622225


SELECT
    AVG(tvl_usd) as avg_tvl_usd,
    AVG(swap_amount_usd) as avg_volume_usd,
    AVG(0.0025 * swap_amount_usd) as avg_fees_usd,
    100 * AVG(365 * 0.0025 * swap_amount_usd / tvl_usd) as avg_apr
FROM balancer.pools_metrics_daily
WHERE blockchain = 'ethereum'
AND project_contract_address = 0x9d1fcf346ea1b073de4d5834e25572cc6ad71f4d
AND block_date >= CURRENT_DATE - INTERVAL '7' DAY
AND block_date < CURRENT_DATE
