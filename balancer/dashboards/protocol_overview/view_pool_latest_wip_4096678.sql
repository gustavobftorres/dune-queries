-- part of a query repo
-- query name: view_pool_latest (WIP)
-- query link: https://dune.com/queries/4096678


SELECT
    m.blockchain,
    m.pool_id,
    m.pool_address,
    m.pool_symbol,
    m.pool_type,
    m.factory_version,
    m.factory_address,
    m.creation_date,
    SUM(CASE WHEN block_date = (SELECT MAX(block_date) FROM balancer.pools_metrics_daily)
    THEN tvl_usd
    END) AS tvl_usd,
    SUM(CASE WHEN block_date = (SELECT MAX(block_date) FROM balancer.pools_metrics_daily)
    THEN tvl_eth
    END) AS tvl_eth,
    SUM(swap_amount_usd) AS swap_volume,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '365' day THEN swap_amount_usd END) AS swap_volume_1y,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '30' day THEN swap_amount_usd END) AS swap_volume_30d,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '7' day THEN swap_amount_usd END) AS swap_volume_7d,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '1' day THEN swap_amount_usd END) AS swap_volume_1d,  
    SUM(fee_amount_usd) AS fees_collected,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '365' day THEN fee_amount_usd END) AS fees_collected_1y,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '30' day THEN fee_amount_usd END) AS fees_collected_30d,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '7' day THEN fee_amount_usd END) AS fees_collected_7d,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '1' day THEN fee_amount_usd END) AS fees_collected_1d  
FROM dune.balancer.result_factory_pool_mapping m
LEFT JOIN balancer.pools_metrics_daily p ON m.blockchain = p.blockchain
AND m.pool_address = p.project_contract_address
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY 9 DESC

/*sources
pools_metrics_daily
volume
fees
liquidity

dune.balancer.result_factory_pool_mapping
pool factory
factory address
pool type
pool id
pool symbol
*/