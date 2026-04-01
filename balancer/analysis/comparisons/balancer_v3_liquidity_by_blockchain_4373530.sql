-- part of a query repo
-- query name: Balancer V3 Liquidity by Blockchain
-- query link: https://dune.com/queries/4373530


    SELECT 
        day,
        blockchain,
        SUM(pool_liquidity_usd) AS tvl
    FROM balancer.liquidity t
    --query_4428144 t
    WHERE 1 = 1
    AND ('{{blockchain}}' = 'All' OR t.blockchain = '{{blockchain}}')
    AND (version = '3')
    GROUP BY 1, 2
