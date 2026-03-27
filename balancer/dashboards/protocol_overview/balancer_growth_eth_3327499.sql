-- part of a query repo
-- query name: Balancer Growth (eth)
-- query link: https://dune.com/queries/3327499


with
sundays AS 
(
    with weeks_seq as (
        SELECT
        sequence(
            (SELECT cast(min(date_trunc('week', day)) - interval '1' day as timestamp) week FROM balancer.liquidity tr)
            , date_trunc('week', cast(now() as timestamp))
            , interval '7' day) as sunday
    )
    
    SELECT 
        weeks.sunday
    FROM weeks_seq
    CROSS JOIN unnest(sunday) as weeks(sunday)
)

SELECT 
    day, 
    pool_liquidity_eth, 
    ((pool_liquidity_eth - LAG(pool_liquidity_eth) OVER (ORDER BY day)) / LAG(pool_liquidity_eth) OVER (ORDER BY day))  as weekly_percentual_variation
FROM 
    (SELECT 
        sunday as day, 
        SUM(protocol_liquidity_eth) as pool_liquidity_eth 
    FROM sundays
    LEFT JOIN balancer.liquidity ON day = sunday
    WHERE day > now() - interval '6' month
    GROUP BY 1) as subquery
ORDER BY 1 DESC

/*select sunday, sum(l1.protocol_liquidity_eth), sum(l2.protocol_liquidity_eth)
FROM sundays
LEFT JOIN balancer.liquidity l1 ON l1.day = sunday
LEFT JOIN balancer.liquidity l2 ON l2.day + interval '7' day = sunday
GROUP BY 1*/

/*select sunday, sum(l2.protocol_liquidity_eth)
FROM sundays
LEFT JOIN balancer.liquidity l2 ON l2.day + interval '7' day = sunday
--WHERE protocol_liquidity_eth IS NOT NULL
GROUP BY 1*/