-- part of a query repo
-- query name: Balancer Snapshot by Version
-- query link: https://dune.com/queries/4353229


    SELECT 
        version,
        blockchain,
        SUM(CASE WHEN block_date = CURRENT_DATE THEN tvl_usd ELSE 0 END) AS tvl_usd,
        SUM(CASE WHEN block_date = CURRENT_DATE THEN tvl_eth ELSE 0 END) AS tvl_eth,
        SUM(CASE WHEN block_date = CURRENT_DATE THEN swap_amount_usd ELSE 0 END) AS today_volume,
        SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' day THEN swap_amount_usd ELSE 0 END) AS volume_7d,
        SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '30' day THEN swap_amount_usd ELSE 0 END) AS volume_30d,
        SUM(swap_amount_usd) AS volume_all_time,
        SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' day THEN fee_amount_usd ELSE 0 END) AS fees_7d,
        SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '30' day THEN fee_amount_usd ELSE 0 END) AS fees_30,
        SUM(fee_amount_usd) AS fees_all_time
    FROM balancer.pools_metrics_daily
    WHERE 1 = 1
    AND ('{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}')
    GROUP BY 1, 2
