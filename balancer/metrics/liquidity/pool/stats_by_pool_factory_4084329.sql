-- part of a query repo
-- query name: Stats by Pool Factory
-- query link: https://dune.com/queries/4084329


    SELECT
        f.factory_version,
        SUM(CASE WHEN block_date = CURRENT_DATE THEN tvl_usd ELSE 0 END) AS total_funds_usd,
        SUM(CASE WHEN block_date = CURRENT_DATE THEN tvl_eth ELSE 0 END) AS total_funds_eth,
        SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '30' day THEN swap_amount_usd ELSE 0 END) AS swap_amount_usd_30d,
        SUM(swap_amount_usd) AS swap_amount_usd_all_time,
        SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '30' day THEN fee_amount_usd ELSE 0 END) AS fee_amount_usd_30d,
        SUM(fee_amount_usd) AS fee_amount_usd_all_time
    FROM query_4080393 f
    LEFT JOIN balancer.pools_metrics_daily l ON f.blockchain = l.blockchain
    AND f.pool_address = l.project_contract_address
    WHERE 1 = 1
    AND ('{{3. Blockchain}}' = 'All' OR l.blockchain = '{{3. Blockchain}}')
    AND ('{{2. Pool Factory}}' = 'All' OR f.factory_version = '{{2. Pool Factory}}')  
    AND f.factory_version IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC