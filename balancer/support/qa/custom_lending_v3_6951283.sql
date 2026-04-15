-- part of a query repo
-- query name: custom_lending_v3
-- query link: https://dune.com/queries/6951283



SELECT blockchain, 
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('bnb', 'ethereum', 'polygon', 'arbitrum', 'fantom', 'avalanche_c')
  AND bytearray_length(code) BETWEEN 10000 AND 50000
  AND (bytearray_position(code, 0x4b8a3529) > 0
    OR bytearray_position(code, 0xc5ebeaec) > 0)
  AND (bytearray_position(code, 0x338b5dea) > 0
    OR bytearray_position(code, 0x47e7ef24) > 0
    OR bytearray_position(code, 0xb6b55f25) > 0)
  AND (bytearray_position(code, 0x27a741ec) > 0
    OR bytearray_position(code, 0x0902f1ac) > 0
    OR bytearray_position(code, 0xfc57d4df) > 0
    OR bytearray_position(code, 0x7dc0d1d0) > 0)
LIMIT 500
