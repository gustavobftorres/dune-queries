-- part of a query repo
-- query name: skytest_strict
-- query link: https://dune.com/queries/6951305



SELECT blockchain, 
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('bnb', 'ethereum', 'polygon', 'arbitrum', 'fantom', 'avalanche_c', 'base', 'optimism')
  AND bytearray_length(code) BETWEEN 10000 AND 50000
  AND bytearray_position(code, 0x4b8a3529) > 0
  AND bytearray_position(code, 0x27a741ec) > 0
LIMIT 500
