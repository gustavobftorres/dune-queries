-- part of a query repo
-- query name: ReCLAMMs Volume by Source
-- query link: https://dune.com/queries/5058179


WITH 
    raw_swaps AS (
        SELECT 
            block_date,
            blockchain,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM balancer.trades b
        JOIN query_5244620 q
        ON q.pool = b.project_contract_address
        AND q.chain = b.blockchain
        AND b.amount_usd IS NOT NULL
        AND b.version = '3'
        GROUP BY 1, 2, 3
    )

SELECT
    s.block_date,
    COALESCE(c.class, 'unknown') AS class,
    SUM(s.volume) AS volume
FROM raw_swaps s
LEFT JOIN dune.balancer.result_balancer_volume_source_classifier c
ON s.channel = c.channel AND s.blockchain = c.blockchain
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

