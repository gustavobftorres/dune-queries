-- part of a query repo
-- query name: Small_chains_-_all_limit_order
-- query link: https://dune.com/queries/6957425



SELECT blockchain, 
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('gnosis','celo','boba','linea','scroll','mantle','blast','opbnb','sonic','kaia','zksync')
  AND bytearray_length(code) BETWEEN 3000 AND 50000
  AND (
    bytearray_position(code, 0x64a3bc15) > 0  -- fillOrder (1inch v3)
    OR bytearray_position(code, 0xe5d7bde6) > 0  -- fillLimitOrder (0x)
    OR bytearray_position(code, 0x3f09e861) > 0  -- executeOrder (PineCore)
    OR bytearray_position(code, 0xd6fc0263) > 0  -- executeOrder variant (JulSwap)
    OR bytearray_position(code, 0x13a76c4c) > 0  -- depositEth (PineCore)
    OR (bytearray_position(code, 0x0e1d31dc) > 0 AND bytearray_position(code, 0xe8ecb130) > 0)  -- createOrder + existOrder
  )
LIMIT 500
