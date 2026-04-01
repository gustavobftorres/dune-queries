-- part of a query repo
-- query name: Balancer V3 Pools Snapshot
-- query link: https://dune.com/queries/4372005


    SELECT 
        m.blockchain,
        m.project_contract_address,
        m.pool_symbol,
        m.pool_type,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM balancer.liquidity) THEN tvl_usd ELSE 0 END) AS tvl_usd,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM balancer.liquidity) THEN tvl_eth ELSE 0 END) AS tvl_eth,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM balancer.liquidity) THEN swap_amount_usd ELSE 0 END) AS today_volume,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '7' day THEN swap_amount_usd ELSE 0 END) AS volume_7d,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '30' day THEN swap_amount_usd ELSE 0 END) AS volume_30d,
        SUM(swap_amount_usd) AS volume_all_time,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '7' day THEN fee_amount_usd ELSE 0 END) AS fees_7d,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '30' day THEN fee_amount_usd ELSE 0 END) AS fees_30,
        SUM(fee_amount_usd) AS daily_fees_usd
    FROM balancer.pools_metrics_daily m
    /*LEFT JOIN query_4428144 q ON q.blockchain = m.blockchain
    AND q.pool_address = m.project_contract_address
    AND q.day = m.block_date*/
    WHERE 1 = 1
    AND ('{{blockchain}}' = 'All' OR m.blockchain = '{{blockchain}}')
    AND m.version = '3'
    GROUP BY 1, 2, 3, 4 
    ORDER BY 5 DESC
