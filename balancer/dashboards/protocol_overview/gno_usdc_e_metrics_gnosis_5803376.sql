-- part of a query repo
-- query name: GNO/USDC.e Metrics (Gnosis)
-- query link: https://dune.com/queries/5803376


SELECT
    SUM(swap_amount_usd) AS total_volume_usd,
    AVG(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' DAY THEN tvl_usd END) as avg_tvl_usd_7d,
    AVG(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' DAY THEN swap_amount_usd END) as avg_volume_usd_7d,
    AVG(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' DAY THEN 0.003 * swap_amount_usd END) as avg_fees_usd_7d,
    100 * AVG(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' DAY THEN 365 * 0.003 * swap_amount_usd / tvl_usd END) as avg_apr_7d
FROM balancer.pools_metrics_daily
WHERE blockchain = 'gnosis'
AND project_contract_address = 0x70b3b56773ace43fe86ee1d80cbe03176cbe4c09
AND block_date < CURRENT_DATE
