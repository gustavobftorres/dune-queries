-- part of a query repo
-- query name: staking_reward_surplus
-- query link: https://dune.com/queries/6954154



SELECT blockchain, 
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('bnb', 'ethereum', 'polygon', 'arbitrum', 'avalanche_c', 'fantom', 'optimism', 'base')
  AND bytearray_length(code) BETWEEN 2000 AND 15000
  AND bytearray_position(code, 0xa694fc3a) > 0
  AND bytearray_position(code, 0x3d18b912) > 0
  AND bytearray_position(code, 0x7b0a47ee) > 0
  AND bytearray_position(code, 0xebe2b12b) > 0
  AND bytearray_position(code, 0x18160ddd) > 0
LIMIT 2000
