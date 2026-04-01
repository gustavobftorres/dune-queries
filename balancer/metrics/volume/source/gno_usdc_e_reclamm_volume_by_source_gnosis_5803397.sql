-- part of a query repo
-- query name: GNO/USDC.e reCLAMM Volume by Source (Gnosis)
-- query link: https://dune.com/queries/5803397


WITH 
    raw_swaps AS (
        SELECT 
            block_date,
            blockchain,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM balancer.trades b
        WHERE blockchain = 'gnosis'
        AND project_contract_address = 0x70b3b56773ace43fe86ee1d80cbe03176cbe4c09
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
