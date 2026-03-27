-- part of a query repo
-- query name: Treasury Revenue Metrics
-- query link: https://dune.com/queries/3237056


WITH 
    "1 Day" AS (
        SELECT 1 AS counter_num, concat('$', format_number(sum(treasury_revenue_usd))) AS counter_metric
        FROM balancer.protocol_fee WHERE day >= NOW() - INTERVAL '24' HOUR 
        AND '{{Blockchain}}' = 'All' or blockchain = '{{Blockchain}}'
        GROUP BY 1
    )
    , "7 Day" AS (
        SELECT 2 AS counter_num, concat('$', format_number(sum(treasury_revenue_usd))) AS counter_metric
        FROM balancer.protocol_fee WHERE day >= NOW() - INTERVAL '7' DAY 
        AND '{{Blockchain}}' = 'All' or blockchain = '{{Blockchain}}'
        GROUP BY 1
    )
    , "30 Day" AS (
        SELECT 3 AS counter_num, concat('$', format_number(sum(treasury_revenue_usd))) AS counter_metric
        FROM balancer.protocol_fee WHERE day >= NOW() - INTERVAL '30' DAY 
        AND '{{Blockchain}}' = 'All' or blockchain = '{{Blockchain}}'
        GROUP BY 1
    )
    , "All Time" AS (
        SELECT 4 AS counter_num, concat('$', format_number(sum(treasury_revenue_usd))) AS counter_metric
        FROM balancer.protocol_fee
        WHERE '{{Blockchain}}' = 'All' or blockchain = '{{Blockchain}}'
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