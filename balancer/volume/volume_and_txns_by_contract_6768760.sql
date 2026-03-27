-- part of a query repo
-- query name: Volume and txns by contract
-- query link: https://dune.com/queries/6768760


SELECT 
    CAST(tx_to as varchar) AS channel,
    d.blockchain,
    SUM(amount_usd) AS volume,
    COUNT(*) AS txns
FROM balancer.trades d
GROUP BY 1, 2