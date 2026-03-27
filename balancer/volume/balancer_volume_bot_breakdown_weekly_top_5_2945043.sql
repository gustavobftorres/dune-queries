-- part of a query repo
-- query name: Balancer Volume (Bot Breakdown) (weekly top 5)
-- query link: https://dune.com/queries/2945043


WITH 
    raw_swaps AS (
        SELECT 
            date_trunc('week', d.block_time) AS week,
            blockchain,
            CAST(tx_to as varchar) AS channel,
            COUNT(*) AS txns,
            sum(amount_usd) AS volume
        FROM dex.trades d
        WHERE project = 'balancer'
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
        AND ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
        AND ('{{6. Pool ID}}' = 'All' OR CAST(project_contract_address as VARCHAR) = '{{6. Pool ID}}')
        AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
        GROUP BY 1, 2, 3
    ),
    
    swaps AS (
        SELECT 
            week,
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
        FROM dex.trades
        WHERE tx_from != 0x0000000000000000000000000000000000000000
        AND project = 'balancer'
        GROUP BY 1,2,3
        ),
        
    heavy_traders AS (
        SELECT
            channel, t.blockchain,  day, daily_trades
        FROM trade_count t
            LEFT JOIN distinct_labels l ON CAST(t.channel as VARCHAR) = l.address AND t.blockchain = l.blockchain
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
    LEFT JOIN labelling ll ON ll.name = l.name AND ll.blockchain = c.blockchain
)
    
SELECT * FROM
(SELECT
    week,
    s.channel,
    c.class,
    s.blockchain,
    CASE WHEN s.blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://etherscan.io/address/0', SUBSTRING(CAST(s.channel as VARCHAR), 2, 41), '">etherscan ↗</a>')
    WHEN s.blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://arbiscan.io/address/0', SUBSTRING(CAST(s.channel as VARCHAR), 2, 41), '">arbiscan ↗</a>')
    WHEN s.blockchain = 'polygon' THEN CONCAT('<a target="_blank" href="https://polygonscan.com/address/0', SUBSTRING(CAST(s.channel as VARCHAR), 2, 41), '">polygonscan ↗</a>')
    WHEN s.blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://gnosisscan.com/address/0', SUBSTRING(CAST(s.channel as VARCHAR), 2, 41), '">gnosisscan ↗</a>')
    WHEN s.blockchain = 'optimism' THEN CONCAT('<a target="_blank" href="https://optimistic.etherscan.io/address/0', SUBSTRING(CAST(s.channel as VARCHAR), 2, 41), '">optimistic ↗</a>')
    WHEN s.blockchain = 'avalanche_c' THEN CONCAT('<a target="_blank" href="https://https://snowtrace.ioio/address/0', SUBSTRING(CAST(s.channel as VARCHAR), 2, 41), '">snowtrace ↗</a>')
    END AS Scan,
    SUM(volume) AS "Volume",
    ROW_NUMBER() OVER (PARTITION BY week ORDER BY sum(volume) DESC NULLS LAST) AS position
FROM swaps s 
INNER JOIN channel_classifier c
ON s.channel = c.channel AND s.blockchain = c.blockchain
WHERE c.class = 'MEV Bot'
GROUP BY 1,2,3,4
ORDER BY 6 DESC) ranking
WHERE position <= 5
ORDER BY "week" DESC, "Volume" DESC