-- part of a query repo
-- query name: Balancer Volume by Token Pair
-- query link: https://dune.com/queries/3457335


-- Volume (token breakdown) per hour (last 24 hours)
-- Visualization: bar chart (stacked)

WITH swaps AS (
        SELECT
            date_trunc('hour', d.block_time) AS hour,
            sum(amount_usd) AS volume,
            COUNT(DISTINCT tx_hash) AS n_swaps,
            CASE WHEN d.token_bought_symbol IS NULL
            THEN CONCAT(CAST(BYTEARRAY_SUBSTRING(token_bought_address, 1, 4) AS VARCHAR), 
            '-', token_sold_symbol)
            WHEN d.token_sold_symbol IS NULL
            THEN CONCAT(CAST(BYTEARRAY_SUBSTRING(token_sold_address, 1, 4) AS VARCHAR), 
            '-', token_bought_symbol)
            ELSE d.token_pair
            END AS token_pair,
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
)

SELECT
    token_pair,
    blockchain,
    sum(s.volume) AS volume,
    SUM(s.n_swaps) AS txs
FROM swaps s
GROUP BY 1, 2
ORDER BY 3 DESC NULLS LAST