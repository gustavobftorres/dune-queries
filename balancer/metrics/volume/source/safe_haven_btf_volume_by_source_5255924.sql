-- part of a query repo
-- query name: Safe Haven BTF - Volume by Source
-- query link: https://dune.com/queries/5255924


WITH 
    raw_swaps AS (
        SELECT 
            DATE_TRUNC('week', block_date) AS week,
            blockchain,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM balancer.trades
        WHERE project_contract_address = 0x6b61d8680c4f9e560c8306807908553f95c749c5
        AND blockchain = 'ethereum'
        AND amount_usd IS NOT NULL
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
