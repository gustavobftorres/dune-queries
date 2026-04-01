-- part of a query repo
-- query name: V3 Trades by CoW Solvers
-- query link: https://dune.com/queries/4515594


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
    block_date,
    s.name,
    SUM(amount_usd) AS volume,
    COUNT(*) AS n_swaps,
    SUM(amount_usd) / COUNT(*) AS avg_swap
FROM balancer.trades t
LEFT JOIN solvers s ON t.tx_from = s.address AND t.blockchain = s.blockchain
WHERE version = '3'
AND tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
AND ('{{blockchain}}' = 'all' OR t.blockchain = '{{blockchain}}')
GROUP BY 1, 2