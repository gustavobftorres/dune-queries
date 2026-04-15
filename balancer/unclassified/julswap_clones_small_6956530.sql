-- part of a query repo
-- query name: julswap_clones_small
-- query link: https://dune.com/queries/6956530



SELECT blockchain, 
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain NOT IN ('ethereum', 'bnb')
  AND bytearray_length(code) BETWEEN 5000 AND 20000
  AND bytearray_position(code, 0x13a76c4c) > 0
  AND bytearray_position(code, 0xe8ecb130) > 0
  AND bytearray_position(code, 0xebd9c39c) > 0
LIMIT 500
