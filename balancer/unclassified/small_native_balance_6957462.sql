-- part of a query repo
-- query name: small_native_balance
-- query link: https://dune.com/queries/6957462



SELECT 
    c.blockchain,
    CAST(c.address AS VARCHAR) as address,
    bytearray_length(c.code) as code_size
FROM evms.contracts c
INNER JOIN evms.balances b 
    ON c.address = b.address 
    AND c.blockchain = b.blockchain
WHERE c.blockchain IN ('gnosis','celo','linea','scroll','mantle','blast','sonic','kaia','boba')
  AND bytearray_length(c.code) BETWEEN 3000 AND 25000
  AND b.token_standard = 'native'
  AND b.balance_raw / 1e18 > 100
ORDER BY b.balance_raw DESC
LIMIT 200
