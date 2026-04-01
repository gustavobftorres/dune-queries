-- part of a query repo
-- query name: Beets DEX Pools Snapshot
-- query link: https://dune.com/queries/4694852


 WITH pool_snaps AS(
    SELECT 
        m.pool_symbol,
        m.pool_type,
        blockchain,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM beets.liquidity) THEN tvl_usd ELSE 0 END) AS tvl_usd,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM beets.liquidity) THEN tvl_eth ELSE 0 END) AS tvl_eth,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM beets.liquidity) THEN swap_amount_usd ELSE 0 END) AS today_volume,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM beets.liquidity) - INTERVAL '7' day THEN swap_amount_usd ELSE 0 END) AS volume_7d,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM beets.liquidity) - INTERVAL '30' day THEN swap_amount_usd ELSE 0 END) AS volume_30d,
        SUM(swap_amount_usd) AS volume_all_time,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM beets.liquidity) - INTERVAL '7' day THEN fee_amount_usd ELSE 0 END) AS fees_7d,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM beets.liquidity) - INTERVAL '30' day THEN fee_amount_usd ELSE 0 END) AS fees_30,
        SUM(fee_amount_usd) AS daily_fees_usd,
        m.project_contract_address
    FROM beets.pools_metrics_daily m
    WHERE 1 = 1
    GROUP BY 1, 2, 3, 13

    UNION

        SELECT 
        m.pool_symbol,
        m.pool_type,
        blockchain,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM beets.liquidity) THEN tvl_usd ELSE 0 END) AS tvl_usd,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM beets.liquidity) THEN tvl_eth ELSE 0 END) AS tvl_eth,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM beets.liquidity) THEN swap_amount_usd ELSE 0 END) AS today_volume,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM beets.liquidity) - INTERVAL '7' day THEN swap_amount_usd ELSE 0 END) AS volume_7d,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM beets.liquidity) - INTERVAL '30' day THEN swap_amount_usd ELSE 0 END) AS volume_30d,
        SUM(swap_amount_usd) AS volume_all_time,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM beets.liquidity) - INTERVAL '7' day THEN fee_amount_usd ELSE 0 END) AS fees_7d,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM beets.liquidity) - INTERVAL '30' day THEN fee_amount_usd ELSE 0 END) AS fees_30,
        SUM(fee_amount_usd) AS daily_fees_usd,
        m.project_contract_address
    FROM beethoven_x_fantom.pools_metrics_daily m
    WHERE 1 = 1
    GROUP BY 1, 2, 3, 13

    UNION ALL
        SELECT 
        m.pool_symbol,
        m.pool_type,
        blockchain,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM beets.liquidity) THEN tvl_usd ELSE 0 END) AS tvl_usd,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM beets.liquidity) THEN tvl_eth ELSE 0 END) AS tvl_eth,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM beets.liquidity) THEN swap_amount_usd ELSE 0 END) AS today_volume,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM beets.liquidity) - INTERVAL '7' day THEN swap_amount_usd ELSE 0 END) AS volume_7d,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM beets.liquidity) - INTERVAL '30' day THEN swap_amount_usd ELSE 0 END) AS volume_30d,
        SUM(swap_amount_usd) AS volume_all_time,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM beets.liquidity) - INTERVAL '7' day THEN fee_amount_usd ELSE 0 END) AS fees_7d,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM beets.liquidity) - INTERVAL '30' day THEN fee_amount_usd ELSE 0 END) AS fees_30,
        SUM(fee_amount_usd) AS daily_fees_usd,
        m.project_contract_address
    FROM balancer.pools_metrics_daily m
    WHERE blockchain = 'optimism'
    GROUP BY 1, 2, 3, 13)

    SELECT * FROM pool_snaps
    WHERE ('{{blockchain}}' = 'All' or blockchain = '{{blockchain}}')
    ORDER BY 4 DESC