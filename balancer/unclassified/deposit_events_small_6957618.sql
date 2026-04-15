-- part of a query repo
-- query name: deposit_events_small
-- query link: https://dune.com/queries/6957618



SELECT l.blockchain,
       CAST(l.contract_address AS VARCHAR) as address,
       COUNT(*) as cnt
FROM evms.logs l
JOIN evms.contracts c ON l.contract_address = c.address AND l.blockchain = c.blockchain
WHERE l.blockchain IN ('blast','sonic','linea','zksync','celo','mantle','kaia','scroll','opbnb','gnosis')
  AND l.topic0 = 0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c
  AND bytearray_length(c.code) BETWEEN 3000 AND 25000
  AND bytearray_length(c.code) != 171
GROUP BY 1, 2
HAVING COUNT(*) >= 5
ORDER BY cnt DESC
LIMIT 200
