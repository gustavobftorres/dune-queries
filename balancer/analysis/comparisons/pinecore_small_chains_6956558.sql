-- part of a query repo
-- query name: pinecore_small_chains
-- query link: https://dune.com/queries/6956558



SELECT blockchain, 
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain NOT IN ('ethereum', 'bnb', 'polygon', 'fantom', 'avalanche_c', 'arbitrum')
  AND bytearray_length(code) BETWEEN 2000 AND 20000
  AND bytearray_position(code, 0x3f09e861) > 0
LIMIT 500
