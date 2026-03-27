-- part of a query repo
-- query name: AAVE/WETH reCLAMM Volume by Source (Arbitrum)
-- query link: https://dune.com/queries/5622030


WITH 
    raw_swaps AS (
        SELECT 
            block_date,
            blockchain,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM balancer.trades b
        WHERE blockchain = 'arbitrum'
        AND project_contract_address = 0x5ea58d57952b028c40bd200e5aff20fc4b590f51
        AND amount_usd IS NOT NULL
        AND version = '3'
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

