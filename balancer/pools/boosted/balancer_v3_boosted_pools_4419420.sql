-- part of a query repo
-- query name: Balancer V3 Boosted Pools
-- query link: https://dune.com/queries/4419420


    SELECT 
        lending_market,
        SUM(CASE WHEN block_date = (SELECT MAX(day) - interval '1' day FROM balancer.liquidity) THEN tvl_usd ELSE 0 END) AS tvl_usd,
        SUM(CASE WHEN block_date = (SELECT MAX(day) - interval '1' day FROM balancer.liquidity) THEN tvl_eth ELSE 0 END) AS tvl_eth,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM balancer.liquidity) THEN swap_amount_usd ELSE 0 END) AS today_volume,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '7' day THEN swap_amount_usd ELSE 0 END) AS volume_7d,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '30' day THEN swap_amount_usd ELSE 0 END) AS volume_30d,
        SUM(swap_amount_usd) AS volume_all_time,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '7' day THEN fee_amount_usd ELSE 0 END) AS fees_7d,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '30' day THEN fee_amount_usd ELSE 0 END) AS fees_30,
        SUM(fee_amount_usd) AS daily_fees_usd
    FROM balancer.pools_metrics_daily m
    INNER JOIN query_4419172 q ON m.project_contract_address = q.address
    AND m.blockchain = q.blockchain
    WHERE 1 = 1
    AND ('{{blockchain}}' = 'All' OR m.blockchain = '{{blockchain}}')
    AND version = '3'
    GROUP BY 1
    ORDER BY 2 DESC
