-- part of a query repo
-- query name: small_chain_orders
-- query link: https://dune.com/queries/6957610



SELECT blockchain,
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('blast','gnosis','sonic','linea','zksync','celo','mantle','kaia','scroll','opbnb')
  AND bytearray_length(code) BETWEEN 3000 AND 20000
  AND bytearray_position(code, 0xd0e30db0) > 0
  AND bytearray_position(code, 0x2e1a7d4d) > 0
  AND (bytearray_position(code, 0x0a19b14a) > 0   -- trade
    OR bytearray_position(code, 0xd6fc0263) > 0     -- executeOrder (JulSwap)
    OR bytearray_position(code, 0x3f09e861) > 0     -- executeOrder (PineCore)
    OR bytearray_position(code, 0x88ec79fb) > 0     -- matchOrders
    OR bytearray_position(code, 0xb4c07547) > 0     -- decodeOrder
    OR bytearray_position(code, 0x338b5dea) > 0     -- depositToken
    OR bytearray_position(code, 0xfb6e155f) > 0     -- availableVolume
    OR bytearray_position(code, 0xe8ecb130) > 0     -- existOrder
    OR bytearray_position(code, 0x514fcac7) > 0)    -- cancelOrder
LIMIT 500
