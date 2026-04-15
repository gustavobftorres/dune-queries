-- part of a query repo
-- query name: bonding_curve_overflow
-- query link: https://dune.com/queries/6955027



SELECT blockchain, 
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('bnb', 'ethereum', 'polygon', 'arbitrum', 'avalanche_c', 'base', 'optimism', 'fantom')
  AND bytearray_length(code) BETWEEN 1500 AND 20000
  AND bytearray_position(code, 0xd96a094a) > 0
  AND (bytearray_position(code, 0xe4849b32) > 0
    OR bytearray_position(code, 0x2e1a7d4d) > 0)
LIMIT 500
