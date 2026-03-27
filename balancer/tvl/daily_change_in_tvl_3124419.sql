-- part of a query repo
-- query name: Daily Change in TVL
-- query link: https://dune.com/queries/3124419


SELECT 
    day
    , CAST(day AS TIMESTAMP) AS day_timestamp
    , CASE WHEN month(current_date) < 10 THEN substring(date_format(CAST(day AS TIMESTAMP), '%m-%d'), 2, 4)
           ELSE date_format(CAST(day AS TIMESTAMP), '%m-%d') 
    END AS formatted_day
    , tvl
    , lag(tvl) OVER(ORDER BY day) AS lag_tvl
    , (tvl - lag(tvl) OVER(ORDER BY day)) / lag(tvl) OVER(ORDER BY day) AS delta
    , ((tvl - lag(tvl) OVER(ORDER BY day)) / lag(tvl) OVER(ORDER BY day)) * 100 AS delta_percentage
    , CASE
        WHEN (tvl - lag(tvl) OVER(ORDER BY day)) / lag(tvl) OVER(ORDER BY day) > 0 THEN '+'
        ELSE '-'
    END AS pos_neg
FROM (
    SELECT 
        distinct
        day, 
        CASE WHEN '{{Currency}}' = 'USD'
        THEN sum(protocol_liquidity_usd) OVER(PARTITION BY day) 
        WHEN '{{Currency}}' = 'eth'
        THEN sum(protocol_liquidity_eth) OVER(PARTITION BY day) 
        END AS tvl
    FROM balancer.liquidity
    WHERE day >= current_date - interval '{{Date Range in Days}}' day
        AND day <= (SELECT MAX(day) FROM balancer.liquidity WHERE version = '2')
)
ORDER BY day DESC, delta