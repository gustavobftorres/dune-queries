-- part of a query repo
-- query name: executeOrder_all
-- query link: https://dune.com/queries/6957451



SELECT blockchain,
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE bytearray_length(code) BETWEEN 2000 AND 30000
  AND bytearray_position(code, 0x3f09e861) > 0
LIMIT 500
