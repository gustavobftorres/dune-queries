-- part of a query repo
-- query name: Balancer 24-hour Volume by Source on Polygon
-- query link: https://dune.com/queries/141906


-- Volume (source breakdown) per week
-- Visualization: bar chart (stacked)

WITH prices AS (
        SELECT date_trunc('hour', minute) AS hour, contract_address AS token, decimals, AVG(price) AS price
        FROM prices.usd
        GROUP BY 1, 2, 3
    ),
    
    swaps AS (
        SELECT 
            date_trunc('hour', block_time) AS hour,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(usd_amount) AS volume
        FROM dex.trades 
        WHERE project = 'Balancer'
        AND block_time > date_trunc('hour', now() - interval '24h')
        AND ('{{1. Pool ID}}' = 'All' OR exchange_contract_address = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea)
        GROUP BY 1, 2
    ),
    
     manual_labels AS (
        SELECT
            address,  
            name
        FROM dune_user_generated.balancer_manual_labels
        WHERE "type" = 'balancer_source'
        AND "author" = 'balancerlabs'
    ),
    
    arb_bots AS (
        SELECT
            address,  
            name
        FROM dune_user_generated.balancer_arb_bots
        WHERE "name" = 'arbitrage bot'
        AND "author" = 'balancerlabs'
        AND address NOT IN (SELECT address from manual_labels)
    ),
    
    distinct_labels AS (
        SELECT * FROM manual_labels
        union all
        SELECT * FROM arb_bots
    ),
    
    channels AS (
        SELECT channel, SUM(COALESCE(volume, 00)) AS volume
        FROM swaps 
        GROUP BY 1
    ),
        
    heavy_traders AS (
        SELECT
            channel, hour, txns AS daily_trades
        FROM swaps
        WHERE txns >= 100
    ),
    
    channel_classifier AS (
        SELECT c.channel, l.name,
            CASE WHEN l.name IS NOT NULL THEN l.name
            WHEN c.channel IN (SELECT channel FROM heavy_traders) THEN 'heavy trader'
            WHEN c.channel IN (select channel from channels where volume is not null order by volume desc limit 10) THEN CONCAT(SUBSTRING(concat('0x', encode(c.channel, 'hex')), 0, 13), '...')
            ELSE 'others' END AS class
        FROM channels c
        LEFT JOIN distinct_labels l ON l.address = c.channel
    )
    
SELECT
    hour,
    c.class,
    sum(txns) AS "Swaps",
    sum(volume)/sum(txns) AS "Avg. Volume per Swap",
    sum(volume) AS "Volume"
FROM swaps s 
INNER JOIN channel_classifier c ON s.channel = c.channel
GROUP BY 1, 2
ORDER BY "Volume" DESC