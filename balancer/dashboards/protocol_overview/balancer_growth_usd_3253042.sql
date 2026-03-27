-- part of a query repo
-- query name: Balancer Growth (USD)
-- query link: https://dune.com/queries/3253042


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
    pool_liquidity_usd, 
    ((pool_liquidity_usd - LAG(pool_liquidity_usd) OVER (ORDER BY day)) / LAG(pool_liquidity_usd) OVER (ORDER BY day))  as weekly_percentual_variation
FROM 
    (SELECT 
        sunday as day, 
        SUM(protocol_liquidity_usd) as pool_liquidity_usd 
    FROM sundays
    LEFT JOIN balancer.liquidity ON day = sunday
    WHERE day > now() - interval '6' month
    GROUP BY 1) as subquery
ORDER BY 1 DESC