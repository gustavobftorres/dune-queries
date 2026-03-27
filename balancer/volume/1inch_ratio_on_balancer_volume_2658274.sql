-- part of a query repo
-- query name: 1inch ratio on Balancer Volume
-- query link: https://dune.com/queries/2658274


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
        SELECT date_trunc('week', d.block_time) AS week,
           json_extract_scalar(trades[1], '$.appData') AS app_id,
            CAST(tx_to as varchar) AS channel,
            COUNT(*) AS txns,
            sum(amount_usd) AS volume
        FROM dex.trades d
        LEFT JOIN gnosis_protocol_v2_ethereum.GPv2Settlement_call_settle g
        ON d.tx_hash = g.call_tx_hash
        WHERE project = 'balancer' --AND blockchain = '{{4. Blockchain}}'
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
        AND ('{{1. Pool ID}}' = 'All' OR CAST(project_contract_address as varchar) = SUBSTRING('{{1. Pool ID}}',1,42))
        GROUP BY 1, 2, 3
    ),
    
    swaps AS (
        SELECT 
            week,
            COALESCE(CAST(g.app_id as varchar), cast(channel as varchar)) AS channel,
            SUM(txns) AS txns,
            SUM(volume) AS volume
        FROM raw_swaps s
        LEFT JOIN gnosis_labels g
        ON g.app_id = s.app_id
        GROUP BY 1, 2
    ),
    
    manual_labels AS (
        SELECT
            CAST(address as varchar) as address,  
            name
        FROM query_2477497
        WHERE "type" = 'balancer_source'
        AND "author" = 'balancerlabs'
        UNION ALL
        SELECT '0x1111111254eeb25477b68fb85ed929f73a960582', '1inch'
         UNION ALL
        SELECT '0xad3b67bca8935cb510c8d18bd45f0b94f54a968f', '1inch'
    ),
    
    arb_bots AS (
        SELECT
            CAST(address as varchar) as address,
            name
        FROM query_2478528
        WHERE "name" = 'arbitrage bot'
        AND "author" = 'balancerlabs'
        AND cast(address as varchar) NOT IN (SELECT address from manual_labels)
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
    
    trade_count AS (
        SELECT
            date_trunc('day', block_time) AS day,
            tx_to AS channel,
            COUNT(1) AS daily_trades
        FROM dex.trades
        WHERE tx_from != 0x0000000000000000000000000000000000000000
        AND project = 'balancer'
        GROUP BY 1,2
        ),
        
    heavy_traders AS (
        SELECT
            channel, day, daily_trades
        FROM trade_count
        WHERE daily_trades >= 100  OR channel IN (0xe001265bdd05a6c84bb3614000c3be7adfd04a83,0xa45d6789ce56128c6064d49a6b1708a716d8fcac)
        ),
    
    channel_classifier AS (
        SELECT 
            l.name,
            c.channel, 
            CASE WHEN g.label IS NOT NULL THEN g.label
            WHEN l.name = 'gnosis_v2' THEN 'gnosis (others)'
            WHEN l.name IS NOT NULL THEN l.name
            WHEN f.pool IS NOT NULL THEN 'BPool direct'
            WHEN c.channel IN (SELECT CAST(channel as varchar) FROM heavy_traders) THEN 'heavy trader'
            WHEN c.channel IN (select channel from channels where volume is not null order by volume desc limit 10) THEN CONCAT(SUBSTRING(concat('0x', to_hex(from_hex(c.channel))), 0, 13), '...')
            ELSE 'others' END AS class
        FROM channels c
        LEFT JOIN balancer_v1_ethereum.BFactory_evt_LOG_NEW_POOL f ON CAST(f.pool as varchar) = c.channel
        LEFT JOIN distinct_labels l ON l.address = c.channel
        LEFT JOIN gnosis_labels g ON g.app_id = c.channel
    ),
oneinchvol as (
SELECT
    week,
    SUM(volume) AS "Volume"
FROM swaps s 
INNER JOIN channel_classifier c
ON s.channel = c.channel
WHERE c.class = '1inch'
GROUP BY 1),   

balancervol as
(SELECT  date_trunc('week', block_time) AS week, sum(amount_usd) as balamount
FROM dex.trades
WHERE project = 'balancer'
AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
        AND ('{{1. Pool ID}}' = 'All' OR CAST(project_contract_address as varchar) = SUBSTRING('{{1. Pool ID}}',1,42))
        GROUP BY 1
)

SELECT sum(i.volume) as "1inch  Volume", sum(b.balamount) as "Balancer Volume", (i.volume)/b.balamount as "1inch Ratio", i.week
FROM oneinchvol i
LEFT JOIN balancervol b ON i.week = b.week
GROUP BY 4, 3