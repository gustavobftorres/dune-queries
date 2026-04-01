-- part of a query repo
-- query name: Balancer Boosted Pools Volume by Source
-- query link: https://dune.com/queries/4661153


WITH 
    raw_swaps AS (
        SELECT 
        CASE 
        WHEN '{{aggregation}}' = 'daily' THEN block_date
        WHEN '{{aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
        WHEN '{{aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
        END AS week,
            t.blockchain,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM balancer.trades t
        INNER JOIN query_4419172 q ON q.address = t.project_contract_address
        AND q.blockchain = t.blockchain
        WHERE amount_usd IS NOT NULL
        AND version = '3'
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

