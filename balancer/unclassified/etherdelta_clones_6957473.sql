-- part of a query repo
-- query name: etherdelta_clones
-- query link: https://dune.com/queries/6957473



SELECT blockchain,
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE bytearray_length(code) BETWEEN 2000 AND 15000
  AND (
    -- EtherDelta/ForkDelta pattern: deposit + trade + withdrawToken
    (bytearray_position(code, 0x0a19b14a) > 0    -- trade()
     AND bytearray_position(code, 0x338b5dea) > 0  -- depositToken
     AND bytearray_position(code, 0xfb6e155f) > 0) -- availableVolume
    -- OR order book with deposit + order matching
    OR (bytearray_position(code, 0x9e281a98) > 0  -- withdrawToken
     AND bytearray_position(code, 0x0a19b14a) > 0  -- trade
     AND bytearray_position(code, 0x2e1a7d4d) > 0) -- withdraw
  )
LIMIT 500
