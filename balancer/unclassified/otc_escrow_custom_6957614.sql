-- part of a query repo
-- query name: otc_escrow_custom
-- query link: https://dune.com/queries/6957614



SELECT blockchain, CAST(address AS VARCHAR) as address, bytearray_length(code) as code_size
FROM evms.contracts
WHERE blockchain IN ('blast','gnosis','sonic','linea','zksync','celo','mantle','kaia','scroll','opbnb',
                     'bnb','polygon','arbitrum','optimism','base','fantom','avalanche_c')
  AND bytearray_length(code) BETWEEN 2000 AND 15000
  AND (
    -- OTC/Swap pattern
    (bytearray_position(code, 0x7bc41b96) > 0 AND bytearray_position(code, 0x2e1a7d4d) > 0)
    -- OR Escrow pattern: deposit + release
    OR (bytearray_position(code, 0xf340fa01) > 0 AND bytearray_position(code, 0x86d1a69f) > 0)
    -- OR generic order with deposit: has both depositToken + some execute + cancelOrder
    OR (bytearray_position(code, 0x338b5dea) > 0 AND bytearray_position(code, 0x514fcac7) > 0)
  )
LIMIT 500
