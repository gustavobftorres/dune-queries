-- part of a query repo
-- query name: Trades by CoW Solvers
-- query link: https://dune.com/queries/4516242


SELECT 
    DATE_TRUNC('week', block_date) AS block_date,
    s.name,
    sum(amount_usd) AS volume,
    COUNT(*) AS n_swaps
FROM balancer.trades t
LEFT JOIN cow_protocol_ethereum.solvers s ON t.tx_from = s.address
WHERE block_date >= now() - interval '3' month
AND tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC