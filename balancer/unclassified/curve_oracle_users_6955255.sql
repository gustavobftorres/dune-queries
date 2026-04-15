-- part of a query repo
-- query name: Curve_oracle_users
-- query link: https://dune.com/queries/6955255



SELECT blockchain, CAST(address AS VARCHAR) as address, bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('bnb','ethereum','polygon','arbitrum','avalanche_c','base','optimism')
  AND bytearray_length(code) BETWEEN 3000 AND 50000
  AND bytearray_position(code, 0xcc2b27d7) > 0
  AND (bytearray_position(code, 0xb6b55f25) > 0
    OR bytearray_position(code, 0x2e1a7d4d) > 0
    OR bytearray_position(code, 0x6e553f65) > 0)
LIMIT 500
