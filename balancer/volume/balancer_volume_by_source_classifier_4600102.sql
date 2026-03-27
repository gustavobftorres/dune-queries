-- part of a query repo
-- query name: Balancer Volume by Source Classifier
-- query link: https://dune.com/queries/4600102


WITH 
    raw_swaps AS (
        SELECT 
            DATE_TRUNC('month', block_date) AS week,
            blockchain,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM balancer.trades
        WHERE amount_usd IS NOT NULL
        GROUP BY 1, 2, 3
    ),
    distinct_labels AS (
        SELECT DISTINCT 
            address,
            blockchain,
            name
        FROM query_3004790
    ),
    channels_with_totals AS (
        SELECT 
            blockchain, 
            channel, 
            SUM(volume) AS volume,
            COUNT(*) AS txns,
            SUM(SUM(volume)) OVER (PARTITION BY blockchain) AS total_volume
        FROM raw_swaps
        GROUP BY 1, 2
    )
    
SELECT 
    c.blockchain,
    c.channel,
    COALESCE(
        CASE 
            WHEN l.name = 'Arbitrage Bot' THEN 'MEV Bot'
            WHEN l.name IS NOT NULL THEN l.name
            WHEN c.txns >= 100 THEN 'Heavy Trader'
            ELSE 'Others'
        END,
        'Others'
    ) AS class
FROM channels_with_totals c
LEFT JOIN distinct_labels l
  ON c.channel = l.address AND c.blockchain = l.blockchain