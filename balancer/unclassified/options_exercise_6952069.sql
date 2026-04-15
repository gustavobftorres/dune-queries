-- part of a query repo
-- query name: options_exercise
-- query link: https://dune.com/queries/6952069



SELECT blockchain, 
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('bnb', 'ethereum', 'polygon', 'arbitrum', 'avalanche_c', 'optimism', 'base')
  AND bytearray_length(code) BETWEEN 2000 AND 30000
  AND (bytearray_position(code, 0xd32cb0fe) > 0
    OR bytearray_position(code, 0xb07f0a41) > 0)
  AND (bytearray_position(code, 0x2e1a7d4d) > 0
    OR bytearray_position(code, 0x3ccfd60b) > 0
    OR bytearray_position(code, 0xa9059cbb) > 0)
LIMIT 500
