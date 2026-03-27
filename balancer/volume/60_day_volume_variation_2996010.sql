-- part of a query repo
-- query name: 60-day Volume Variation
-- query link: https://dune.com/queries/2996010


SELECT 
    day,
    pool_volume_usd, 
    ((pool_volume_usd - LAG(pool_volume_usd) OVER (ORDER BY day)) / LAG(pool_volume_usd) OVER (ORDER BY day))  as daily_percentual_variation
FROM 
    (SELECT 
        block_date as day, 
        sum(amount_usd) as pool_volume_usd 
    FROM dex.trades
    WHERE block_date > now() - interval '60' day
        AND project = 'balancer'
    GROUP BY 
        block_date) as subquery
ORDER BY day;