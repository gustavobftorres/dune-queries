-- part of a query repo
-- query name: Balancer Volume by CoW Solver
-- query link: https://dune.com/queries/4600250


WITH solvers AS(
    SELECT
        'ethereum' AS blockchain,
        address,
        name
    FROM cow_protocol_ethereum.solvers
    
    UNION 
    
    SELECT
        'gnosis' AS blockchain,
        address,
        name
    FROM cow_protocol_gnosis.solvers

    UNION 
    
    SELECT
        'arbitrum' AS blockchain,
        address,
        name
    FROM cow_protocol_arbitrum.solvers    

    UNION 
    
    SELECT
        'base' AS blockchain,
        address,
        name
    FROM cow_protocol_base.solvers       
)

SELECT 
    CASE 
        WHEN '{{aggregation}}' = 'daily' THEN block_date
        WHEN '{{aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
        WHEN '{{aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
    END AS block_date,
    s.name,
    SUM(amount_usd) AS volume,
    COUNT(*) AS n_swaps,
    SUM(amount_usd) / COUNT(*) AS avg_swap
FROM balancer.trades t
LEFT JOIN solvers s ON t.tx_from = s.address AND t.blockchain = s.blockchain
WHERE 1 = 1 
AND tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
AND block_date >= TIMESTAMP '{{start_date}}'
AND t.blockchain IN ({{blockchain}})
AND ('{{balancer_token_pair}}' = 'All' OR token_pair = '{{balancer_token_pair}}')
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC