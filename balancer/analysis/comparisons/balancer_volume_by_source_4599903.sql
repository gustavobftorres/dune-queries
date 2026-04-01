-- part of a query repo
-- query name: Balancer Volume by Source
-- query link: https://dune.com/queries/4599903


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
        WHERE amount_usd IS NOT NULL
        AND block_date >= TIMESTAMP '{{start_date}}'
        AND blockchain IN ({{blockchain}})
        AND ('{{balancer_token_pair}}' = 'All' OR token_pair = '{{balancer_token_pair}}')
        GROUP BY 1, 2, 3
    )

SELECT
    s.week,
    c.class,
    SUM(s.volume) AS volume
FROM raw_swaps s
INNER JOIN dune.balancer.result_balancer_volume_source_classifier c
  ON s.channel = c.channel AND s.blockchain = c.blockchain
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

