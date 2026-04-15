-- part of a query repo
-- query name: compound_forks_donation
-- query link: https://dune.com/queries/6955070



SELECT blockchain, 
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('bnb', 'ethereum', 'polygon', 'arbitrum', 'avalanche_c', 'base', 'optimism', 'fantom')
  AND bytearray_length(code) BETWEEN 5000 AND 30000
  AND bytearray_position(code, 0x3b1d21a2) > 0
  AND bytearray_position(code, 0x47bd3718) > 0
  AND bytearray_position(code, 0x182df0f5) > 0
  AND bytearray_position(code, 0xa0712d68) > 0
  AND bytearray_position(code, 0x6f307dc3) > 0
LIMIT 2000
