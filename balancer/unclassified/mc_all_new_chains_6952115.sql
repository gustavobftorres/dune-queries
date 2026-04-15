-- part of a query repo
-- query name: mc_all_new_chains
-- query link: https://dune.com/queries/6952115



SELECT blockchain, 
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('blast', 'gnosis', 'sonic', 'opbnb', 'linea', 'zksync', 'celo', 'mantle', 'kaia', 'scroll')
  AND bytearray_length(code) BETWEEN 3000 AND 30000
  AND bytearray_position(code, 0xe2bbb158) > 0
  AND bytearray_position(code, 0x441a3e70) > 0
  AND bytearray_position(code, 0x1526fe27) > 0
LIMIT 500
