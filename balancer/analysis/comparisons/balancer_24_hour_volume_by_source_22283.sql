-- part of a query repo
-- query name: Balancer 24-hour Volume by Source
-- query link: https://dune.com/queries/22283


-- Volume (source breakdown) per hour (last 24 hours)
-- Visualization: bar chart (stacked)

WITH gnosis_labels AS (
        SELECT 
            '0x0000000000000000000000000000000000000000000000000000000000000001' AS app_id,
            'gnosis (balancer)' AS label
        UNION ALL
        SELECT 
            '0x0000000000000000000000000000000000000000000000000000000000000002' AS app_id,
            'gnosis (balancer)' AS label
        UNION ALL
        SELECT 
            '0xe9f29ae547955463ed535162aefee525d8d309571a2b18bc26086c8c35d781eb' AS app_id,
            'gnosis (balancer)' AS label
        UNION ALL
        SELECT 
            '0x487b02c558d729abaf3ecf17881a4181e5bc2446429a0995142297e897b6eb37' AS app_id,
            'gnosis (cowswap)' AS label
        UNION ALL
        SELECT
            '0xe4d1ab10f2c9ffe7bdd23c315b03f18cff90888d6b2bb5022bacd46ab9cddf24' AS app_id,
            'gnosis (cowswap)' AS label
    ),
    
    raw_swaps AS (
        SELECT date_trunc('hour', d.block_time) AS hour,
            (trades -> 0 ->> 'appData') AS app_id,
            tx_to AS channel,
            COUNT(*) AS txns,
            sum(usd_amount) AS volume
        FROM dex.trades d
        LEFT JOIN gnosis_protocol_v2."GPv2Settlement_call_settle" g
        ON d.tx_hash = g.call_tx_hash
        WHERE project = 'Balancer'
        AND date_trunc('hour', d.block_time) > date_trunc('hour', now() - interval '1 day')
        AND ('{{4. Version}}' = 'Both' OR version = SUBSTRING('{{4. Version}}', 2))
        AND ('{{1. Pool ID}}' = 'All' OR exchange_contract_address = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea)
        GROUP BY 1, 2, 3
    ),
    
    swaps AS (
        SELECT 
            hour,
            COALESCE(g.app_id::bytea, channel) AS channel,
            SUM(txns) AS txns,
            SUM(volume) AS volume
        FROM raw_swaps s
        LEFT JOIN gnosis_labels g
        ON g.app_id = s.app_id
        GROUP BY 1, 2
    ),
    
    manual_labels AS (
        SELECT
            address,  
            name
        FROM labels.labels
        WHERE "type" = 'balancer_source'
        AND "author" = 'balancerlabs'
    ),
    
    arb_bots AS (
        SELECT
            address,  
            name
        FROM labels.labels
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
        SELECT channel, sum(coalesce(volume,00)) as volume from swaps group by 1
    ),
    
    trade_count AS (
        SELECT
            date_trunc('day', block_time) AS day,
            tx_to AS channel,
            COUNT(1) AS daily_trades
        FROM dex.trades
        WHERE trader_a != '\x0000000000000000000000000000000000000000'
        AND project = 'Balancer'
        GROUP BY 1,2
        ),
        
    heavy_traders AS (
        SELECT
            channel, day, daily_trades
        FROM trade_count
        WHERE daily_trades >= 100
        ),
    
    channel_classifier AS (
        SELECT 
            l.name,
            c.channel, 
            CASE WHEN g.label IS NOT NULL THEN g.label
            WHEN l.name = 'gnosis_v2' THEN 'gnosis (others)'
            WHEN l.name IS NOT NULL THEN l.name
            WHEN f.pool IS NOT NULL THEN 'BPool direct'
            WHEN c.channel IN (SELECT channel FROM heavy_traders) THEN 'heavy trader'
            WHEN c.channel IN (select channel from channels where volume is not null order by volume desc limit 10) THEN CONCAT(SUBSTRING(concat('0x', encode(c.channel, 'hex')), 0, 13), '...')
            ELSE 'others' END AS class
        FROM channels c
        LEFT JOIN balancer."BFactory_evt_LOG_NEW_POOL" f ON f.pool = c.channel
        LEFT JOIN distinct_labels l ON l.address = c.channel
        LEFT JOIN gnosis_labels g ON g.app_id::bytea = c.channel
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