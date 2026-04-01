-- part of a query repo
-- query name: AAVE/WETH reCLAMM Volume by Source (Ethereum)
-- query link: https://dune.com/queries/5531592


WITH 
    raw_swaps AS (
        SELECT 
            block_date,
            blockchain,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM balancer.trades b
        WHERE blockchain = 'ethereum'
        AND project_contract_address = 0x9d1fcf346ea1b073de4d5834e25572cc6ad71f4d
        AND block_date >= timestamp '2025-11-01'
        AND amount_usd IS NOT NULL
        AND version = '3'
        GROUP BY 1, 2, 3
    )

SELECT
    s.block_date,
    COALESCE(c.class, 'Others') AS class,
    SUM(s.volume) AS volume
FROM raw_swaps s
LEFT JOIN dune.balancer.result_balancer_volume_source_classifier c
ON s.channel = c.channel AND s.blockchain = c.blockchain
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
