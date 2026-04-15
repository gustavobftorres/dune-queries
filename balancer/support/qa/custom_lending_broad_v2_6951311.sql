-- part of a query repo
-- query name: custom_lending_broad_v2
-- query link: https://dune.com/queries/6951311



SELECT blockchain, 
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('bnb', 'ethereum', 'polygon', 'arbitrum', 'fantom', 'avalanche_c', 'base', 'optimism')
  AND bytearray_length(code) BETWEEN 10000 AND 50000
  AND bytearray_position(code, 0x4b8a3529) > 0
  AND (bytearray_position(code, 0x7dc0d1d0) > 0
    OR bytearray_position(code, 0xf11993df) > 0
    OR bytearray_position(code, 0x98d5fdca) > 0)
  AND bytearray_position(code, 0x40c10f19) = 0
  AND bytearray_position(code, 0xa0712d68) = 0
LIMIT 500
