-- part of a query repo
-- query name: MEV Capture Volume by Source
-- query link: https://dune.com/queries/5436053


WITH 
    raw_swaps AS (
        SELECT 
            DATE_TRUNC('week', block_date) AS week,
            blockchain,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM balancer.trades
        WHERE amount_usd IS NOT NULL
        AND project_contract_address = 0xd0bfa4784285acd49e06e12f302c2441c5923bfd
        AND blockchain = 'base'
        AND version = '3'
        GROUP BY 1, 2, 3
    )

SELECT
    s.week,
    c.class,
    SUM(s.volume) AS volume
FROM raw_swaps s
JOIN dune.balancer.result_balancer_volume_source_classifier c
ON s.channel = c.channel AND s.blockchain = c.blockchain
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
