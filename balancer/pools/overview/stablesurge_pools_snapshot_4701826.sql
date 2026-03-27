-- part of a query repo
-- query name: StableSurge Pools Snapshot
-- query link: https://dune.com/queries/4701826


WITH threshold AS( 
    SELECT 
        chain,
        pool,
        evt_block_time,
        newSurgeThresholdPercentage / POWER(10,18) AS surge_threshold_percentage,
        ROW_NUMBER() OVER (PARTITION BY pool, chain ORDER BY evt_block_time DESC) AS rn
    FROM balancer_v3_multichain.stablesurgehook_evt_thresholdsurgepercentagechanged
    WHERE 1 = 1),

    max AS(
        SELECT 
        chain,
        pool,
        evt_block_time,
        newMaxSurgeFeePercentage / POWER(10,18) AS max_surge_fee_percentage,
        ROW_NUMBER() OVER (PARTITION BY pool, chain ORDER BY evt_block_time DESC) AS rn
    FROM balancer_v3_multichain.stablesurgehook_evt_maxsurgefeepercentagechanged
    WHERE 1 = 1
    AND ('{{blockchain}}' = 'All' OR chain = '{{blockchain}}')
    )
 
 SELECT 
        m.blockchain,
        pool_symbol,
        surge_threshold_percentage,
        max_surge_fee_percentage,
        amplificationParameter AS amp_parameter,
        SUM(CASE WHEN block_date = (SELECT MAX(day) - interval '1' day FROM balancer.liquidity) THEN tvl_usd ELSE 0 END) AS tvl_usd,
        SUM(CASE WHEN block_date = (SELECT MAX(day) - interval '1' day FROM balancer.liquidity) THEN tvl_eth ELSE 0 END) AS tvl_eth,
        SUM(CASE WHEN block_date = (SELECT MAX(day) FROM balancer.liquidity) THEN swap_amount_usd ELSE 0 END) AS today_volume,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '7' day THEN swap_amount_usd ELSE 0 END) AS volume_7d,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '30' day THEN swap_amount_usd ELSE 0 END) AS volume_30d,
        SUM(swap_amount_usd) AS volume_all_time,
        SUM(CASE WHEN block_date >= (SELECT MAX(day) FROM balancer.liquidity) - INTERVAL '30' day THEN fee_amount_usd ELSE 0 END) AS fees_30,
        SUM(fee_amount_usd) AS fees_all_time,        
        project_contract_address
    FROM balancer.pools_metrics_daily m
    INNER JOIN balancer_v3_multichain.stablesurgepoolfactory_call_create q ON m.project_contract_address = q.output_pool
    AND m.blockchain = q.chain
    JOIN threshold t ON q.chain = t.chain 
    AND q.output_pool = t.pool
    AND t.rn = 1
    JOIN max x ON q.chain = x.chain 
    AND q.output_pool = x.pool
    AND x.rn = 1
    WHERE 1 = 1
    AND ('{{blockchain}}' = 'All' OR m.blockchain = '{{blockchain}}')
    GROUP BY 1, 2, 3, 4, 5, 14
    ORDER BY 6 DESC
