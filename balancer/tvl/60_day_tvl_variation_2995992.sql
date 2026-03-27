-- part of a query repo
-- query name: 60-day TVL variation
-- query link: https://dune.com/queries/2995992


SELECT 
    day, 
    pool_liquidity_eth, 
    ((pool_liquidity_eth - LAG(pool_liquidity_eth) OVER (ORDER BY day)) / LAG(pool_liquidity_eth) OVER (ORDER BY day))  as daily_percentual_variation
FROM 
    (SELECT 
        DATE_TRUNC('week', CAST(day as timestamp)) as day, 
        protocol_liquidity_eth as pool_liquidity_eth 
    FROM balancer.liquidity
    WHERE day > now() - interval '60' day
    GROUP BY 1, 2) as subquery
ORDER BY 
    day;