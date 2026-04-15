-- part of a query repo
-- query name: matchOrders_DEX
-- query link: https://dune.com/queries/6957533



SELECT blockchain,
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE bytearray_length(code) BETWEEN 5000 AND 40000
  AND bytearray_position(code, 0x88ec79fb) > 0
  AND (bytearray_position(code, 0x2e1a7d4d) > 0
    OR bytearray_position(code, 0x9e281a98) > 0
    OR bytearray_position(code, 0xa9059cbb) > 0)
LIMIT 500
