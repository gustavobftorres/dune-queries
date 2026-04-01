-- part of a query repo
-- query name: Protocol Fees Collected Breakdown
-- query link: https://dune.com/queries/3228155


WITH data_30d AS(
    SELECT 
    blockchain,
    pool_address,
    SUM(protocol_fee_collected_usd) as protocol_30d,
    SUM(treasury_fee_usd) as treasury_30d
    FROM balancer.protocol_fee
    WHERE day >= now() - interval '30' day
    GROUP BY 1, 2
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
    day_rate * pct_votes AS emissions
FROM bal_supply b
LEFT JOIN gauge_votes v on v.day = b.day
LEFT JOIN labels.balancer_gauges m ON v.gauge = m.address
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
)

SELECT 
    p.pool_symbol, 
    p.pool_type,
    p.version,
    SUM(p.protocol_fee_collected_usd) AS protocol, 
    t.protocol_30d AS protocol_30d,
    total_emissions AS emissions_all_time,
    emissions_30d AS emissions_30d,
    SUM(p.treasury_fee_usd) AS tres,
    t.treasury_30d AS treasury_30d,
      p.blockchain || 
        CASE 
            WHEN p.blockchain = 'arbitrum' THEN ' 🟦'
            WHEN p.blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN p.blockchain = 'base' THEN ' 🟨'
            WHEN p.blockchain = 'ethereum' THEN ' Ξ'
            WHEN p.blockchain = 'gnosis' THEN ' 🟩'
            WHEN p.blockchain = 'optimism' THEN ' 🔴'
            WHEN p.blockchain = 'polygon' THEN ' 🟪'
            WHEN p.blockchain = 'zkevm' THEN ' 🟣'
        END 
    AS blockchain,        
    CASE
            WHEN p.blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/ethereum/pool/', CAST("pool_id" AS VARCHAR), '">balancer ↗</a>')
            WHEN p.blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/arbitrum/pool/', CAST("pool_id" AS VARCHAR), '">balancer ↗</a>')
            WHEN p.blockchain = 'polygon' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/polygon/pool/', CAST("pool_id" AS VARCHAR), '">balancer ↗</a>')
            WHEN p.blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/gnosis-chain/pool/', CAST("pool_id" AS VARCHAR), '">balancer ↗</a>')
            WHEN p.blockchain = 'base' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/base/pool/', CAST("pool_id" AS VARCHAR), '">balancer ↗</a>')
            WHEN p.blockchain = 'avalanche_c' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/avalanche/pool/', CAST("pool_id" AS VARCHAR), '">balancer ↗</a>')
            WHEN p.blockchain = 'optimism' THEN CONCAT('<a target="_blank" href="https://beets.fi/pool/', CAST("pool_id" AS VARCHAR), '">beethoven ↗</a>')
            WHEN p.blockchain = 'zkevm' THEN CONCAT('<a target="_blank" href="https://app.balancer.fi/#/zkevm/pool/', CAST("pool_id" AS VARCHAR), '">balancer ↗</a>')
        END AS view_pool,
    p.pool_address
FROM balancer.protocol_fee p
LEFT JOIN data_30d t 
ON t.pool_address = p.pool_address 
AND t.blockchain = p.blockchain 
LEFT JOIN bal_emissions b ON b.pool_address = p.pool_address AND b.blockchain = p.blockchain
LEFT JOIN dune.balancer.dataset_core_pools c 
ON c.network = p.blockchain AND c.pool = p.pool_id
WHERE ('{{Blockchain}}' = 'All' OR p.blockchain = '{{Blockchain}}')
AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
AND ('{{Pool Type}}' = 'All' OR p.pool_type = '{{Pool Type}}')
GROUP BY 1, 2, 3, 5, 6, 7, 9, 10, 11, 12
HAVING SUM(p.protocol_fee_collected_usd) > 0 
ORDER BY 5 DESC