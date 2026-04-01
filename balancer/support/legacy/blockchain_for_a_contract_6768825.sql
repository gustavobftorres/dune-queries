-- part of a query repo
-- query name: Blockchain for a contract
-- query link: https://dune.com/queries/6768825


SELECT CAST(tx_to as varchar) AS channel, blockchain
FROM balancer.trades
WHERE tx_from != 0x0000000000000000000000000000000000000000
GROUP BY 1, 2
HAVING COUNT(*) >= 100