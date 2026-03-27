-- part of a query repo
-- query name: Base Macro BTF - Volume by Source
-- query link: https://dune.com/queries/5255953


WITH 
    raw_swaps AS (
        SELECT 
            DATE_TRUNC('week', block_date) AS week,
            blockchain,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM balancer.trades
        WHERE project_contract_address = 0xb4161aea25bd6c5c8590ad50deb4ca752532f05d
        AND blockchain = 'base'
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
