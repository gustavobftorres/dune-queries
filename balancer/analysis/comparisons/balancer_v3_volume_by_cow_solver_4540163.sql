-- part of a query repo
-- query name: Balancer V3 Volume by CoW Solver
-- query link: https://dune.com/queries/4540163


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
)

SELECT 
    DATE_TRUNC('week', block_date) AS block_date,
    s.name,
    SUM(amount_usd) AS volume,
    COUNT(*) AS n_swaps,
    SUM(amount_usd) / COUNT(*) AS avg_swap
FROM balancer.trades t
LEFT JOIN solvers s ON t.tx_from = s.address AND t.blockchain = s.blockchain
WHERE version = '3'
AND tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
AND ('{{blockchain}}' = 'All' OR t.blockchain = '{{blockchain}}')
GROUP BY 1, 2