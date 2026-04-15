-- part of a query repo
-- query name: small_chain_deposits
-- query link: https://dune.com/queries/6957456



WITH contract_balances AS (
    SELECT 
        t.blockchain,
        CAST(t.to AS VARCHAR) as address,
        SUM(CAST(t.value AS DOUBLE)) / 1e18 as total_received
    FROM evms.transactions t
    JOIN evms.contracts c ON t.to = c.address AND t.blockchain = c.blockchain
    WHERE t.blockchain IN ('gnosis','celo','boba','linea','scroll','mantle','blast','sonic','kaia')
      AND t.value > 0
      AND bytearray_length(c.code) BETWEEN 3000 AND 25000
    GROUP BY 1, 2
    HAVING SUM(CAST(t.value AS DOUBLE)) / 1e18 > 10
)
SELECT blockchain, address, total_received
FROM contract_balances
ORDER BY total_received DESC
LIMIT 200
