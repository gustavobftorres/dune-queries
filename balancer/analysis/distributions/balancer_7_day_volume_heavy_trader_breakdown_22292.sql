-- part of a query repo
-- query name: Balancer 7-day Volume (Heavy Trader Breakdown)
-- query link: https://dune.com/queries/22292


-- Volume (source breakdown) per day (last 7 days)
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
        SELECT date_trunc('day', d.block_time) AS day,
            (trades -> 0 ->> 'appData') AS app_id,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(usd_amount) AS volume
        FROM dex.trades d
        LEFT JOIN gnosis_protocol_v2."GPv2Settlement_call_settle" g
        ON d.tx_hash = g.call_tx_hash
        WHERE project = 'Balancer'
        AND date_trunc('day', d.block_time) > now() - interval '1 week'
        AND ('{{4. Version}}' = 'Both' OR version = SUBSTRING('{{4. Version}}', 2))
        AND ('{{1. Pool ID}}' = 'All' OR exchange_contract_address = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea)
        GROUP BY 1, 2, 3
    ),
    
    swaps AS (
        SELECT 
            day,
            COALESCE(g.app_id::bytea, channel) AS channel,
            SUM(txns) AS txns,
            SUM(volume) AS volume
        FROM raw_swaps s
        LEFT JOIN gnosis_labels g
        ON g.app_id = s.app_id
        GROUP BY 1, 2
    ),
    
    labels AS (
        SELECT
            address,
            name
        FROM labels.labels
        WHERE ("type" = 'balancer_source'
        OR "name" = 'arbitrage bot')
        AND "author" = 'balancerlabs'
    ),
    
    distinct_labels AS (
        SELECT * FROM (SELECT address, name, COUNT(*)
        FROM labels
        GROUP BY address, name
        HAVING COUNT(*) > 1) d
        WHERE "name" <> 'arbitrage bot'
        UNION ALL
        SELECT address, name, COUNT(*)
        FROM labels
        GROUP BY address, name
        HAVING COUNT(*) = 1
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
            WHEN c.channel IN (select channel from channels where volume is not null order by volume desc limit 10) THEN concat('0x', encode(c.channel, 'hex'))
            ELSE 'others' END AS class
        FROM channels c
        LEFT JOIN balancer."BFactory_evt_LOG_NEW_POOL" f ON f.pool = c.channel
        LEFT JOIN labels l ON l.address = c.channel
        LEFT JOIN gnosis_labels g ON g.app_id::bytea = c.channel
    )
    
    SELECT
        day,
        s.channel,
        CONCAT('<a target="_blank" href="https://etherscan.io/address/0', SUBSTRING(s.channel::text, 2, 42), '">', 'https://etherscan.io/address/0', SUBSTRING(s.channel::text, 2, 42), '</a>') AS etherscan,
        s.txns AS trades,
        sum(volume) AS "Volume"
    FROM swaps s 
    INNER JOIN channel_classifier c ON s.channel = c.channel
    WHERE c.class = 'heavy trader'
    GROUP BY 1, 2, 3, 4
    ORDER BY day DESC, "Volume" DESC