-- part of a query repo
-- query name: Balancer Volume by Source
-- query link: https://dune.com/queries/4545850


WITH 
    raw_swaps AS (
        SELECT 
            CASE 
                WHEN '{{aggregation}}' = 'daily' THEN block_date
                WHEN '{{aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
                WHEN '{{aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
            END AS week,
            blockchain,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM balancer.trades
        WHERE ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
          --AND version = '3'
          AND block_date > now() - interval '6' month
          AND amount_usd IS NOT NULL
        GROUP BY 1, 2, 3
    ),

    distinct_labels AS (
        SELECT 
            DISTINCT 
            address,
            blockchain,
            name
        FROM query_3004790
    ),

    channels AS (
        SELECT 
            blockchain, 
            channel, 
            SUM(volume) AS volume
        FROM raw_swaps
        GROUP BY 1, 2
    ),

    total_volume AS (
        SELECT 
            SUM(volume) AS total_volume 
        FROM channels
    ),

    labelling AS (
        SELECT 
            l.blockchain,
            l.name,
            SUM(c.volume) AS volume
        FROM distinct_labels l
        LEFT JOIN channels c
          ON l.address = c.channel AND l.blockchain = c.blockchain
        GROUP BY 1, 2
    ),

    heavy_traders AS (
        SELECT
            t.channel, 
            t.blockchain
        FROM raw_swaps t
        LEFT JOIN distinct_labels l
          ON t.channel = l.address
          AND t.blockchain = l.blockchain
        WHERE txns >= 100
          AND l.name IS NULL
    ),

    channel_classifier AS (
        SELECT 
            l.name,
            c.blockchain,
            c.channel, 
            CASE 
                WHEN l.name IN ('Vault', 'Direct Router')   THEN 'FE/programmatic'
                ELSE 'Others'
            END AS class
        FROM channels c
        LEFT JOIN distinct_labels l
          ON l.address = c.channel AND l.blockchain = c.blockchain
    )

SELECT
    s.week,
    c.class,
    SUM(s.volume) AS "Volume"
FROM raw_swaps s
INNER JOIN channel_classifier c
  ON s.channel = c.channel AND s.blockchain = c.blockchain
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
