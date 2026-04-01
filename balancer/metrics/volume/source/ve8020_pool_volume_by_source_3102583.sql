-- part of a query repo
-- query name: ve8020 Pool Volume by Source
-- query link: https://dune.com/queries/3102583


WITH 
    raw_swaps AS (
        SELECT 
            date_trunc('month', d.block_time) AS month,
            blockchain,
            CAST(tx_to as varchar) AS channel,
            COUNT(*) AS txns,
            sum(amount_usd) AS volume
        FROM balancer.trades d
        WHERE project = 'balancer'
        AND blockchain = '{{Blockchain}}'
        AND project_contract_address = {{Pool Address}}
        GROUP BY 1, 2, 3
    ),
    
      swaps AS (
        SELECT 
            month,
            blockchain,
            cast(channel as varchar) AS channel,
            SUM(txns) AS txns,
            SUM(volume) AS volume
        FROM raw_swaps s
        GROUP BY 1, 2,3
    ),
    
    distinct_labels AS (
        SELECT 
        CAST(address as varchar) as address,
        blockchain,
        name
        FROM query_3004790 l
    ),
    
    labelling AS (
        SELECT 
        s.blockchain,
        l.name,
        sum(s.volume) as volume
        FROM distinct_labels l
        LEFT JOIN swaps s ON l.address = s.channel AND l.blockchain = s.blockchain 
        GROUP BY 1,2
    ),
    
    channels AS (
        SELECT blockchain, channel, SUM(COALESCE(volume, 00)) AS volume
        FROM swaps
        GROUP BY 1, 2
    ),
    
    trade_count AS (
        SELECT
            date_trunc('day', block_time) AS day,
            blockchain,
            tx_to AS channel,
            COUNT(1) AS daily_trades
        FROM balancer.trades
        WHERE tx_from != 0x0000000000000000000000000000000000000000
        GROUP BY 1,2,3
        ),
        
    heavy_traders AS (
        SELECT
            channel, t.blockchain, day, daily_trades
        FROM trade_count t
            LEFT JOIN distinct_labels l ON CAST(t.channel as VARCHAR) = l.address
            AND t.blockchain = l.blockchain
        WHERE daily_trades >= 100  AND l.name IS NULL
        ),
    
channel_classifier AS (
    SELECT 
        DISTINCT l.name,
        c.blockchain,
        c.channel, 
        CASE 
            WHEN l.name = 'Arbitrage Bot' THEN 'MEV Bot'
            WHEN l.name IS NOT NULL AND COALESCE(ll.volume, 0) / (SELECT SUM(volume) FROM channels) >= 0.005 THEN l.name 
            WHEN l.name IS NOT NULL AND COALESCE(ll.volume, 0) / (SELECT SUM(volume) FROM channels) < 0.005 THEN 'Other DEXs and Aggregators'
            WHEN l.name IS NULL AND c.channel IN (SELECT CAST(channel as varchar) FROM heavy_traders) THEN 'Heavy Trader'
            ELSE 'Others'
        END AS class
    FROM channels c
    LEFT JOIN distinct_labels l ON l.address = c.channel AND l.blockchain = c.blockchain
    LEFT JOIN labelling ll ON ll.name = l.name AND ll.blockchain = l.blockchain
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
ORDER BY 1 DESC, 5 DESC