-- part of a query repo
-- query name: limit_order_broad_v3
-- query link: https://dune.com/queries/6957443



SELECT blockchain,
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE bytearray_length(code) BETWEEN 3000 AND 25000
  AND (
    -- Pattern: has both create + execute/fill order
    (bytearray_position(code, 0x0e1d31dc) > 0 AND bytearray_position(code, 0x3f09e861) > 0)
    -- Pattern: has deposit + execute + some order management
    OR (bytearray_position(code, 0xd0e30db0) > 0 AND bytearray_position(code, 0x3f09e861) > 0)
    -- Pattern: createLimitOrder + fillLimitOrder (non-1inch)
    OR (bytearray_position(code, 0x17fc98f2) > 0 AND bytearray_position(code, 0xe4dcb06b) > 0)
    -- Pattern: placeOrder + cancelOrder + fillOrder (generic DEX)
    OR (bytearray_position(code, 0xfe18f830) > 0 AND bytearray_position(code, 0x514fcac7) > 0)
  )
  AND bytearray_position(code, 0x13a76c4c) = 0  -- NOT PineCore
  AND bytearray_position(code, 0x64a3bc15) = 0  -- NOT 1inch
LIMIT 500
