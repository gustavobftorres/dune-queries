-- part of a query repo
-- query name: Perp_DEX_positions
-- query link: https://dune.com/queries/6955261



SELECT blockchain, CAST(address AS VARCHAR) as address, bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('arbitrum','optimism','base')
  AND bytearray_length(code) BETWEEN 5000 AND 50000
  AND bytearray_position(code, 0x48d91abf) > 0
  AND bytearray_position(code, 0x90205d8c) > 0
LIMIT 500
