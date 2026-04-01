-- part of a query repo
-- query name: Gyro Pool Metrics
-- query link: https://dune.com/queries/4582991


WITH bal_price AS(
SELECT
    date_trunc('day', minute) AS day,
    APPROX_PERCENTILE(price, 0.5) AS median_price
FROM prices.usd
WHERE blockchain = 'ethereum'
AND symbol = 'BAL'
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
    gauge,
    symbol,
    pct_votes
FROM query_756468
LEFT JOIN days ON DATE_TRUNC('week', day) = DATE_TRUNC('week', CAST(start_date AS TIMESTAMP))
),

daily_bal_emissions AS(
SELECT 
    b.day,
    gauge,
    symbol,
    m.pool_address,
    m.blockchain,    
    day_rate * pct_votes * median_price AS emissions
FROM bal_supply b
LEFT JOIN gauge_votes v on v.day = b.day
LEFT JOIN labels.balancer_gauges m ON v.gauge = m.address
LEFT JOIN bal_price p ON p.day = b.day
WHERE symbol IS NOT NULL
),

bal_emissions AS(
    SELECT
        pool_address,
        blockchain,
        SUM(emissions) AS total_emissions,
        SUM(CASE WHEN day >= now() - INTERVAL '30' DAY THEN emissions ELSE 0 END) AS emissions_30d
    FROM daily_bal_emissions b
    GROUP BY 1, 2
),

swap_fees AS(
SELECT
    project_contract_address,
    blockchain,
    SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' day THEN swap_fee * amount_usd END) AS seven_day_swap_fee_usd,
    SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '30' day THEN swap_fee * amount_usd END) AS thirty_day_swap_fee_usd,
    SUM(swap_fee * amount_usd) AS all_time_swap_fee_usd
FROM balancer.trades
WHERE pool_type = 'ECLP'
GROUP BY 1, 2
)

SELECT
    m.blockchain,
    pool_symbol,
    m.project_contract_address,
    CASE WHEN c.symbol IS NOT NULL THEN 'Core'
    ELSE 'Non-Core' END AS core_pool,
    SUM(CASE WHEN block_date = CURRENT_DATE THEN tvl_usd END) AS tvl_usd,
    SUM(CASE WHEN block_date = CURRENT_DATE THEN tvl_eth END) AS tvl_eth,
    SUM(CASE WHEN block_date = CURRENT_DATE THEN swap_amount_usd END) AS today_volume_usd,
    SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' day THEN swap_amount_usd END) AS seven_day_volume_usd,
    SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '30' day THEN swap_amount_usd END) AS thirty_day_volume_usd,
    SUM(swap_amount_usd) AS all_time_volume_usd,
    SUM(CASE WHEN block_date = CURRENT_DATE THEN fee_amount_usd END) AS today_fee_usd,
    SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '7' day THEN fee_amount_usd END) AS seven_day_fee_usd,
    SUM(CASE WHEN block_date >= CURRENT_DATE - INTERVAL '30' day THEN fee_amount_usd END) AS thirty_day_fee_usd,
    SUM(fee_amount_usd) AS all_time_fee_usd,
    seven_day_swap_fee_usd,
    thirty_day_swap_fee_usd,
    all_time_swap_fee_usd,
    emissions_30d AS thirty_day_emissions_usd,
    total_emissions AS all_time_emissions_usd
FROM balancer.pools_metrics_daily m 
LEFT JOIN bal_emissions e ON m.project_contract_address = pool_address
AND m.blockchain = e.blockchain
LEFT JOIN swap_fees f ON m.project_contract_address = f.project_contract_address
AND m.blockchain = f.blockchain
LEFT JOIN dune.balancer.dataset_core_pools c ON BYTEARRAY_SUBSTRING(c.pool, 1, 20) = m.project_contract_address
AND c.network = m.blockchain
WHERE pool_type = 'ECLP'
GROUP BY 1, 2, 3, 4, 15, 16, 17, 18, 19
ORDER BY 5 DESC