-- part of a query repo
-- query name: COW/WETH Metrics (Mainnet)
-- query link: https://dune.com/queries/5808129


SELECT
    SUM(swap_amount_usd) AS total_volume_usd,
    AVG(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' DAY THEN tvl_usd END) as avg_tvl_usd_7d,
    AVG(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' DAY THEN swap_amount_usd END) as avg_volume_usd_7d,
    AVG(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' DAY THEN 0.003 * swap_amount_usd END) as avg_fees_usd_7d,
    100 * AVG(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' DAY THEN 365 * 0.003 * swap_amount_usd / tvl_usd END) as avg_apr_7d
FROM balancer.pools_metrics_daily
WHERE blockchain = 'ethereum'
AND project_contract_address = 0xd321300ef77067d4a868f117d37706eb81368e98
AND block_date < CURRENT_DATE
