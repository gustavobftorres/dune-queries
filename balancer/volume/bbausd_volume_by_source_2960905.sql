-- part of a query repo
-- query name: bbaUSD Volume by  Source
-- query link: https://dune.com/queries/2960905


WITH 
    raw_swaps AS (
        SELECT 
            date_trunc('month', d.block_time) AS month,
            CAST(tx_to as varchar) AS channel,
            COUNT(*) AS txns,
            sum(amount_usd) AS volume
        FROM dex.trades d
        WHERE project = 'balancer'
        AND project_contract_address  IN (
        0xa13a9247ea42d743238089903570127dda72fe44,
        0x7b50775383d3d6f0215a8f290f2c9e2eebbeceb2
  )
        GROUP BY 1, 2--, 3
    ),
    
    swaps AS (
        SELECT 
            month,
            cast(channel as varchar) AS channel,
            SUM(txns) AS txns,
            SUM(volume) AS volume
        FROM raw_swaps s
        GROUP BY 1, 2
    ),
    
    distinct_labels AS (
        SELECT 
        CAST(address as varchar) as address,
        name 
        FROM query_2478528
    ),
    
    channels AS (
        SELECT channel, SUM(COALESCE(volume, 00)) AS volume
        FROM swaps
        GROUP BY 1
    ),
    
    trade_count AS (
        SELECT
            date_trunc('month', block_time) AS month,
            tx_to AS channel,
            COUNT(1) AS daily_trades
        FROM dex.trades
        WHERE tx_from != 0x0000000000000000000000000000000000000000
        AND project = 'balancer'
        GROUP BY 1,2
        ),
        
    heavy_traders AS (
        SELECT
            channel, month, daily_trades
        FROM trade_count
        WHERE daily_trades >= 100  
        OR channel IN (0xeefa9b99109ec96e589c84b2bee7d64be2be020b, 0xe001265bdd05a6c84bb3614000c3be7adfd04a83,
                       0xa45d6789ce56128c6064d49a6b1708a716d8fcac, 0x73d4679f03b29b96e14d8b699ca5f48d43777f39)
        ),
    
    channel_classifier AS (
        SELECT 
            l.name,
            c.channel, 
            CASE 
            WHEN l.name IS NOT NULL THEN l.name
            WHEN f.pool IS NOT NULL THEN 'BPool direct'
            WHEN c.channel IN (SELECT CAST(channel as varchar) FROM heavy_traders) THEN 'heavy trader'
            WHEN c.channel IN (select channel from channels where volume is not null order by volume desc limit 10) THEN CONCAT(SUBSTRING(concat('0x', to_hex(from_hex(c.channel))), 1, 13), '...')
            ELSE 'others' END AS class
        FROM channels c
        LEFT JOIN balancer_v1_ethereum.BFactory_evt_LOG_NEW_POOL f ON CAST(f.pool as varchar) = c.channel
        LEFT JOIN distinct_labels l ON l.address = c.channel
    )
    
SELECT
    month,
    c.class,
    SUM(txns) AS "Swaps",
    SUM(volume)/SUM(txns) AS "Avg. Volume per Swap",
    SUM(volume) AS "Volume"
FROM swaps s 
INNER JOIN channel_classifier c
ON s.channel = c.channel
GROUP BY 1,2
ORDER BY 4 DESC