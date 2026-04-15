-- part of a query repo
-- query name: Conditional_orders
-- query link: https://dune.com/queries/6957537



SELECT blockchain,
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE bytearray_length(code) BETWEEN 3000 AND 30000
  AND bytearray_position(code, 0x1cff79cd) > 0
  AND bytearray_position(code, 0x514fcac7) > 0
LIMIT 500
