-- part of a query repo
-- query name: Medium_chains_-_non-PineCore
-- query link: https://dune.com/queries/6957426



SELECT blockchain,
       CAST(address AS VARCHAR) as address,
       bytearray_length(code) as code_size
FROM evms.contracts  
WHERE blockchain IN ('arbitrum','optimism','base','avalanche_c','fantom','polygon')
  AND bytearray_length(code) BETWEEN 3000 AND 30000
  AND (
    bytearray_position(code, 0xd6fc0263) > 0  -- JulSwap executeOrder
    OR bytearray_position(code, 0xe55060c5) > 0  -- JulSwap executeOrder variant
  )
  AND bytearray_position(code, 0xebd9c39c) = 0  -- NOT PineCore (exclude already checked)
LIMIT 500
