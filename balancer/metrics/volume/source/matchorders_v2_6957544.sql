-- part of a query repo
-- query name: matchOrders_v2
-- query link: https://dune.com/queries/6957544



SELECT blockchain,
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE bytearray_length(code) BETWEEN 5000 AND 40000
  AND bytearray_position(code, 0x88ec79fb) > 0
LIMIT 300
