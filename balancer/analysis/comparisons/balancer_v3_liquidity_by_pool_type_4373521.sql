-- part of a query repo
-- query name: Balancer V3 Liquidity by Pool Type
-- query link: https://dune.com/queries/4373521


    SELECT 
        day,
        pool_type,
        SUM(pool_liquidity_usd) AS tvl
    FROM balancer.liquidity t
    --query_4428144 t
    WHERE 1 = 1
    AND ('{{blockchain}}' = 'All' OR t.blockchain = '{{blockchain}}')
    AND (version = '3')
    GROUP BY 1, 2
