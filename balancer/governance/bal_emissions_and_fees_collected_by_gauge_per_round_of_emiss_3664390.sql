-- part of a query repo
-- query name: BAL Emissions and Fees Collected By Gauge, per Round of Emissions with check
-- query link: https://dune.com/queries/3664390


WITH 

pool_labels AS(
    SELECT
        blockchain,
        address,
        CASE WHEN pool_type IN ('WP', 'WP2T')
        THEN 'Weighted'
        WHEN pool_type IN ('SP')
        THEN 'Stable'
        WHEN pool_type IN ('IP')
        THEN 'Investment'   
        WHEN pool_type IN ('MP')
        THEN 'Managed'
        ELSE pool_type
        END AS pool_type
    FROM labels.balancer_v2_pools
),

bal_price AS(
SELECT 
    DATE_TRUNC('day', minute) AS day,
    APPROX_PERCENTILE(price, 0.5) AS price
FROM prices.usd
WHERE symbol = 'BAL' AND blockchain = 'ethereum'
GROUP BY 1
),

bal_supply AS(
SELECT 
    time AS day,
    DATE_TRUNC('week', time) AS week,
    day_rate,
    week_rate
FROM query_2846023
),

days AS 
(
    with days_seq AS (
        SELECT
        sequence(
            (SELECT CAST(min(DATE_TRUNC('day', CAST(start_date AS timestamp))) AS timestamp) day FROM query_756468 tr)
            , DATE_TRUNC('day', CAST(now() AS timestamp))
            , interval '1' day) AS day
    )
    
    SELECT 
        days.day
    FROM days_seq
    CROSS JOIN unnest(day) AS days(day)
),

gauge_votes AS(
SELECT
    day + interval '3' day AS day, --workaround for daily votes
    round_id,
    start_date,
    end_date,
    gauge,
    symbol,
    pct_votes
FROM query_756468
LEFT JOIN days ON DATE_TRUNC('week', day) = DATE_TRUNC('week', CAST(start_date AS TIMESTAMP))
),

daily_bal_emissions AS(
SELECT 
    b.day,
    start_date,
    end_date,
    round_id,
    gauge,
    symbol,
    day_rate * pct_votes AS emissions
FROM bal_supply b
LEFT JOIN gauge_votes v on v.day = b.day
WHERE symbol IS NOT NULL
),

daily_bal_emissions_and_fees AS(
SELECT 
    b.day,
    round_id,
    start_date, 
    end_date,
    b.gauge,
    m.pool_address,
    m.blockchain,
    b.symbol,
    emissions AS daily_emissions,
    emissions * price AS daily_emissions_usd,
    SUM(protocol_fee_collected_usd) AS daily_fees
FROM daily_bal_emissions b
LEFT JOIN dune.balancer.result_gauge_to_pool_mapping m ON m.gauge_address = b.gauge
LEFT JOIN balancer.protocol_fee f ON 
f.pool_address = m.pool_address AND 
f.blockchain = m.blockchain AND 
f.day = b.day
LEFT JOIN bal_price p ON p.day = b.day
WHERE symbol IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

SELECT 
    round_id,
    start_date,
    end_date,
    d.blockchain,
    gauge,
    pool_address,
    d.symbol,
    SUM(SUM(daily_emissions)) OVER (PARTITION BY gauge, round_id ORDER BY round_id) AS round_emissions,
    SUM(SUM(daily_emissions_usd)) OVER (PARTITION BY gauge, round_id ORDER BY round_id) AS round_emissions_usd,
    SUM(SUM(daily_fees)) OVER (PARTITION BY gauge, round_id ORDER BY round_id) AS round_fees,
    SUM(SUM(daily_fees)) OVER (PARTITION BY gauge, round_id ORDER BY round_id) / SUM(SUM(daily_emissions_usd)) OVER (PARTITION BY gauge, round_id ORDER BY round_id) AS ratio,
    CASE WHEN SUM(SUM(daily_fees)) OVER (PARTITION BY gauge, round_id ORDER BY round_id) / SUM(SUM(daily_emissions_usd)) OVER (PARTITION BY gauge, round_id ORDER BY round_id) > 1.1
    THEN TRUE
    ELSE FALSE
    END AS check
FROM daily_bal_emissions_and_fees d
LEFT JOIN dune.balancer.dataset_core_pools c 
ON c.network = d.blockchain AND BYTEARRAY_SUBSTRING(c.pool,1,20) = d.pool_address
LEFT JOIN pool_labels l 
ON l.blockchain = d.blockchain AND l.address = d.pool_address
WHERE day < CURRENT_DATE
AND day >= TIMESTAMP '{{Start Date}}'
AND ('{{Pool Address}}' = 'All' OR CAST(pool_address AS VARCHAR) = '{{Pool Address}}')
AND ('{{Gauge}}' = 'All' OR CAST(gauge AS VARCHAR) = '{{Gauge}}')
AND ('{{Blockchain}}' = 'All' OR d.blockchain = '{{Blockchain}}')
AND ('{{Round ID}}' = 'All' OR CAST(round_id AS VARCHAR) = '{{Round ID}}')
AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
AND ('{{Pool Type}}' = 'All' OR l.pool_type = '{{Pool Type}}')
GROUP BY 1, 2, 3, 4, 5, 6, 7
HAVING SUM(daily_fees) > 1 AND SUM(daily_emissions) > 1
ORDER BY 1 DESC, 8 DESC, 9 DESC