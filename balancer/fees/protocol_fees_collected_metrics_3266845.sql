-- part of a query repo
-- query name: Protocol Fees Collected Metrics
-- query link: https://dune.com/queries/3266845


WITH 
    "1 Day" AS (
        SELECT 1 AS counter_num, concat('$', format_number(sum(protocol_fee_collected_usd))) AS counter_metric
        FROM balancer.protocol_fee f
        LEFT JOIN dune.balancer.dataset_core_pools c 
        ON c.network = f.blockchain AND c.pool = f.pool_id 
        WHERE day = CURRENT_DATE - INTERVAL '1' DAY 
        AND ('{{Blockchain}}' = 'All' or f.blockchain = '{{Blockchain}}')
        AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
        AND ('{{Pool Type}}' = 'All' OR f.pool_type = '{{Pool Type}}')
        AND protocol_fee_collected_usd < 1000000000
        GROUP BY 1
    )
    , "7 Day" AS (
        SELECT 2 AS counter_num, concat('$', format_number(sum(protocol_fee_collected_usd))) AS counter_metric
        FROM balancer.protocol_fee f
        LEFT JOIN dune.balancer.dataset_core_pools c 
        ON c.network = f.blockchain AND c.pool = f.pool_id 
        WHERE day >= NOW() - INTERVAL '7' DAY 
        AND ('{{Blockchain}}' = 'All' or f.blockchain = '{{Blockchain}}')
        AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
        AND ('{{Pool Type}}' = 'All' OR f.pool_type = '{{Pool Type}}')
        AND protocol_fee_collected_usd < 1000000000
        GROUP BY 1
    )
    , "30 Day" AS (
        SELECT 3 AS counter_num, concat('$', format_number(sum(protocol_fee_collected_usd))) AS counter_metric
        FROM balancer.protocol_fee f
        LEFT JOIN dune.balancer.dataset_core_pools c 
        ON c.network = f.blockchain AND c.pool = f.pool_id 
        WHERE day >= NOW() - INTERVAL '30' DAY 
        AND ('{{Blockchain}}' = 'All' or f.blockchain = '{{Blockchain}}')
        AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
        AND ('{{Pool Type}}' = 'All' OR f.pool_type = '{{Pool Type}}')
        AND protocol_fee_collected_usd < 1000000000
        GROUP BY 1
    )
    , "All Time" AS (
        SELECT 4 AS counter_num, concat('$', format_number(sum(protocol_fee_collected_usd))) AS counter_metric
        FROM balancer.protocol_fee f
        LEFT JOIN dune.balancer.dataset_core_pools c 
        ON c.network = f.blockchain AND c.pool = f.pool_id 
        WHERE ('{{Blockchain}}' = 'All' or f.blockchain = '{{Blockchain}}')
        AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
        AND ('{{Pool Type}}' = 'All' OR f.pool_type = '{{Pool Type}}')
        AND protocol_fee_collected_usd < 1000000000
        GROUP BY 1
    )
    
SELECT * FROM "1 Day"
UNION
SELECT * FROM "7 Day"
UNION
SELECT * FROM "30 Day"
UNION
SELECT * FROM "All Time"
ORDER BY counter_num ASC