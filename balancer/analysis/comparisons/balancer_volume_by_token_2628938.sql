-- part of a query repo
-- query name: Balancer Volume by Token
-- query link: https://dune.com/queries/2628938


-- Volume (token breakdown) per hour (last 24 hours)
-- Visualization: bar chart (stacked)

WITH swaps AS (
        SELECT
            date_trunc('hour', d.block_time) AS hour,
            sum(amount_usd) AS volume,
            COUNT(DISTINCT tx_hash) AS n_swaps,
            CAST(d.token_bought_address as varchar) AS address,
            CONCAT( CASE 
            WHEN d.blockchain = 'arbitrum' THEN ' 🟦 |'
            WHEN d.blockchain = 'avalanche_c' THEN ' ⬜  |'
            WHEN d.blockchain = 'base' THEN ' 🟨 |'
            WHEN d.blockchain = 'ethereum' THEN ' Ξ |'
            WHEN d.blockchain = 'gnosis' THEN ' 🟩 |'
            WHEN d.blockchain = 'optimism' THEN ' 🔴 |'
            WHEN d.blockchain = 'polygon' THEN ' 🟪 |'
            WHEN d.blockchain = 'zkevm' THEN ' 🟣 |'
            END 
            , ' ', d.token_bought_symbol) AS token,
            d.blockchain
        FROM balancer.trades d
        WHERE (('{{4. Blockchain}}' = 'All' AND 1 = 1) OR d.blockchain = '{{4. Blockchain}}')
        AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
        AND (
          ('{{Aggregation}}' = 'Last 24 hours' AND date_trunc('hour', d.block_time) >= date_trunc('hour', now() - interval '1' day)) OR
          ('{{Aggregation}}' = 'Last 30 days' AND date_trunc('day', d.block_time) >= date_trunc('day', now() - interval '1' month)) OR
          ('{{Aggregation}}' = 'Last 90 days' AND date_trunc('week', d.block_time) >= date_trunc('week', now() - interval '3' month)) OR
          ('{{Aggregation}}' = 'Last 365 days' AND date_trunc('month', d.block_time) >= date_trunc('week', now() - interval '1' year))
        )                
        GROUP BY 1, 4, 5, 6
        
        UNION ALL
        
        SELECT
            date_trunc('hour', d.block_time) AS hour,
            sum(amount_usd) AS volume,
            COUNT(DISTINCT tx_hash) AS n_swaps,            
            CAST(d.token_sold_address as varchar) AS address,
            CONCAT( CASE 
            WHEN d.blockchain = 'arbitrum' THEN ' 🟦 |'
            WHEN d.blockchain = 'avalanche_c' THEN ' ⬜  |'
            WHEN d.blockchain = 'base' THEN ' 🟨 |'
            WHEN d.blockchain = 'ethereum' THEN ' Ξ |'
            WHEN d.blockchain = 'gnosis' THEN ' 🟩 |'
            WHEN d.blockchain = 'optimism' THEN ' 🔴 |'
            WHEN d.blockchain = 'polygon' THEN ' 🟪 |'
            WHEN d.blockchain = 'zkevm' THEN ' 🟣 |'
            END 
            , ' ', d.token_sold_symbol) AS token,
            d.blockchain
        FROM balancer.trades d
        WHERE (('{{4. Blockchain}}' = 'All' AND 1 = 1) OR d.blockchain = '{{4. Blockchain}}')
        AND (
          ('{{Aggregation}}' = 'Last 24 hours' AND date_trunc('hour', d.block_time) >= date_trunc('hour', now() - interval '1' day)) OR
          ('{{Aggregation}}' = 'Last 30 days' AND date_trunc('day', d.block_time) >= date_trunc('day', now() - interval '1' month)) OR
          ('{{Aggregation}}' = 'Last 90 days' AND date_trunc('week', d.block_time) >= date_trunc('week', now() - interval '3' month)) OR
          ('{{Aggregation}}' = 'Last 365 days' AND date_trunc('month', d.block_time) >= date_trunc('week', now() - interval '1' year))
        )        
        GROUP BY 1, 4, 5, 6
)

SELECT
    COALESCE(s.token, CONCAT(SUBSTRING(s.address, 1, 10), '...')) AS token,
    blockchain,
    CASE WHEN blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://etherscan.io/address/0', SUBSTRING(CAST("address" as VARCHAR), 2, 41), '">etherscan ↗</a>')
    WHEN blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://arbiscan.io/address/0', SUBSTRING(CAST("address" as VARCHAR), 2, 41), '">arbiscan ↗</a>')
    WHEN blockchain = 'polygon' THEN CONCAT('<a target="_blank" href="https://polygonscan.com/address/0', SUBSTRING(CAST("address" as VARCHAR), 2, 41), '">polygonscan ↗</a>')
    WHEN blockchain = 'gnosis' THEN CONCAT('<a target="_blank" href="https://gnosisscan.com/address/0', SUBSTRING(CAST("address" as VARCHAR), 2, 41), '">gnosisscan ↗</a>')
    WHEN blockchain = 'optimism' THEN CONCAT('<a target="_blank" href="https://optimistic.etherscan.io/address/0', SUBSTRING(CAST("address" as VARCHAR), 2, 41), '">optimistic ↗</a>')
    WHEN blockchain = 'avlanche_c' THEN CONCAT('<a target="_blank" href="https://snowtrace.io/address/0', SUBSTRING(CAST("address" as VARCHAR), 2, 41), '">snowtrace ↗</a>')
    WHEN blockchain = 'base' THEN CONCAT('<a target="_blank" href="https://basescan.org/address/0', SUBSTRING(CAST("address" as VARCHAR), 2, 41), '">basescan ↗</a>')
    WHEN blockchain = 'zkevm' THEN CONCAT('<a target="_blank" href="https://zkevm.polygonscan.com/address/0', SUBSTRING(CAST("address" as VARCHAR), 2, 41), '">polygonscan ↗</a>')
    END AS Scan,
    sum(s.volume)/2 AS volume,
    SUM(s.n_swaps) AS txs,
    address
FROM swaps s
GROUP BY 1, 2, 3, 6
ORDER BY 4 DESC NULLS LAST