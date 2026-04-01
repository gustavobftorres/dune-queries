-- part of a query repo
-- query name: Factory <> Pool Mapping
-- query link: https://dune.com/queries/4080393


WITH all_pools AS (

  -- ═══════════════════════════════════════════
  -- WEIGHTED POOLS (with ordinality zip pattern)
  -- ═══════════════════════════════════════════

  SELECT 'ethereum' AS blockchain, cc.contract_address AS factory_address, c.poolId AS pool_id,
    t.tokens AS token_address, w.weights / pow(10,18) AS normalized_weight, cc.symbol, 'weighted' AS pool_type
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_ethereum.WeightedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'ethereum', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_ethereum.WeightedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'ethereum', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_ethereum.WeightedPoolV2Factory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'ethereum', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_ethereum.WeightedPool2TokensFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'ethereum', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'investment'
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_ethereum.InvestmentPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos

  UNION ALL
  SELECT 'arbitrum', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_arbitrum.WeightedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'arbitrum', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_arbitrum.WeightedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'arbitrum', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_arbitrum.WeightedPoolV2Factory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'arbitrum', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'investment'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_arbitrum.InvestmentPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'arbitrum', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_arbitrum.WeightedPool2TokensFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos

  UNION ALL
  SELECT 'avalanche_c', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_avalanche_c.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_avalanche_c.WeightedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos

  UNION ALL
  SELECT 'base', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_base.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_base.WeightedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos

  UNION ALL
  SELECT 'gnosis', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_gnosis.WeightedPoolV2Factory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'gnosis', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_gnosis.WeightedPoolV4Factory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos

  UNION ALL
  SELECT 'optimism', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_optimism.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_optimism.WeightedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'optimism', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_optimism.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_optimism.WeightedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'optimism', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_optimism.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_optimism.WeightedPoolV2Factory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'optimism', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_optimism.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_optimism.WeightedPool2TokensFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos

  UNION ALL
  SELECT 'polygon', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_polygon.WeightedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'polygon', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_polygon.WeightedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'polygon', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_polygon.WeightedPoolV2Factory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'polygon', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_polygon.WeightedPool2TokensFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos
  UNION ALL
  SELECT 'polygon', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'investment'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_polygon.InvestmentPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos

  UNION ALL
  SELECT 'zkevm', cc.contract_address, c.poolId, t.tokens, w.weights / pow(10,18), cc.symbol, 'weighted'
  FROM balancer_v2_zkevm.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_zkevm.WeightedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
  CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos) WHERE t.pos = w.pos

  -- ═══════════════════════════════════════════
  -- STABLE POOLS
  -- ═══════════════════════════════════════════

  UNION ALL SELECT 'ethereum', cc.contract_address, c.poolId, t.tokens AS token_address, 0 AS normalized_weight, cc.symbol, 'stable'
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_ethereum.StablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'ethereum', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_ethereum.MetaStablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'ethereum', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_ethereum.ComposableStablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'arbitrum', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_arbitrum.StablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'arbitrum', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_arbitrum.MetaStablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'arbitrum', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_arbitrum.ComposableStablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'avalanche_c', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_avalanche_c.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_avalanche_c.ComposableStablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'base', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_base.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_base.ComposableStablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'gnosis', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_gnosis.StablePoolV2Factory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'gnosis', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_gnosis.ComposableStablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'gnosis', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_gnosis.ComposableStablePoolV2Factory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'optimism', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_optimism.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_optimism.StablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'optimism', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_optimism.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_optimism.MetaStablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'optimism', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_optimism.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_optimism.ComposableStablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'polygon', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_polygon.StablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'polygon', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_polygon.MetaStablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'polygon', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_polygon.ComposableStablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'polygon', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_polygon.StablePhantomPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'zkevm', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'stable'
  FROM balancer_v2_zkevm.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_zkevm.ComposableStablePoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  -- ═══════════════════════════════════════════
  -- LBP POOLS
  -- ═══════════════════════════════════════════

  UNION ALL SELECT 'ethereum', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'LBP'
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_ethereum.LiquidityBootstrappingPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'ethereum', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'LBP'
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_ethereum.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'ethereum', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'LBP'
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_ethereum.StablePhantomPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'arbitrum', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'LBP'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_arbitrum.LiquidityBootstrappingPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'arbitrum', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'LBP'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_arbitrum.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'avalanche_c', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'LBP'
  FROM balancer_v2_avalanche_c.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_avalanche_c.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'base', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'LBP'
  FROM balancer_v2_base.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_base.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'gnosis', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'LBP'
  FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_gnosis.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'optimism', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'LBP'
  FROM balancer_v2_optimism.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_optimism.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'polygon', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'LBP'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_polygon.LiquidityBootstrappingPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'polygon', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'LBP'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_polygon.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL SELECT 'zkevm', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'LBP'
  FROM balancer_v2_zkevm.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_zkevm.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  -- ═══════════════════════════════════════════
  -- LINEAR POOLS
  -- ═══════════════════════════════════════════

  UNION ALL SELECT 'ethereum', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_ethereum.AaveLinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)

  UNION ALL SELECT 'arbitrum', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_arbitrum.AaveLinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)
  UNION ALL SELECT 'arbitrum', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_arbitrum.ERC4626LinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)

  UNION ALL SELECT 'avalanche_c', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_avalanche_c.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_avalanche_c.AaveLinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)
  UNION ALL SELECT 'avalanche_c', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_avalanche_c.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_avalanche_c.ERC4626LinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)

  UNION ALL SELECT 'base', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_base.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_base.AaveLinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)
  UNION ALL SELECT 'base', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_base.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_base.ERC4626LinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)

  UNION ALL SELECT 'gnosis', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_gnosis.AaveLinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)
  UNION ALL SELECT 'gnosis', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_gnosis.AaveLinearPoolV3Factory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)

  UNION ALL SELECT 'optimism', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_optimism.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_optimism.AaveLinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)

  UNION ALL SELECT 'polygon', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_polygon.AaveLinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)

  UNION ALL SELECT 'zkevm', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_zkevm.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_zkevm.AaveLinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)
  UNION ALL SELECT 'zkevm', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_zkevm.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_zkevm.YearnLinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)
  UNION ALL SELECT 'zkevm', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_zkevm.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_zkevm.GearboxLinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)
  UNION ALL SELECT 'zkevm', cc.contract_address, c.poolId, t.element, 0, cc.symbol, 'linear'
  FROM balancer_v2_zkevm.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_zkevm.ERC4626LinearPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t(element)

  -- ═══════════════════════════════════════════
  -- ECLP POOLS
  -- ═══════════════════════════════════════════

  UNION ALL SELECT 'ethereum', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'ECLP'
  FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c INNER JOIN gyroscope_ethereum.GyroECLPPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'arbitrum', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'ECLP'
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c INNER JOIN gyroscope_arbitrum.GyroECLPPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'base', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'ECLP'
  FROM balancer_v2_base.Vault_evt_PoolRegistered c INNER JOIN gyroscope_base.GyroECLPPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'gnosis', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'ECLP'
  FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c INNER JOIN gyroscope_gnosis.GyroECLPPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'optimism', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'ECLP'
  FROM balancer_v2_optimism.Vault_evt_PoolRegistered c INNER JOIN gyroscope_optimism.GyroECLPPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_pool
  CROSS JOIN UNNEST(cc.tokens_array_binary) AS t(tokens)
  UNION ALL SELECT 'polygon', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'ECLP'
  FROM balancer_v2_polygon.Vault_evt_PoolRegistered c INNER JOIN gyroscope_polygon.GyroECLPPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
  UNION ALL SELECT 'zkevm', cc.contract_address, c.poolId, t.tokens, 0, cc.symbol, 'ECLP'
  FROM balancer_v2_zkevm.Vault_evt_PoolRegistered c INNER JOIN gyroscope_zkevm.GyroECLPPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  -- ═══════════════════════════════════════════
  -- FX POOLS
  -- ═══════════════════════════════════════════

  UNION ALL SELECT 'ethereum', cc.contract_address, cc.output_0, t.token, 0, cc._name, 'FX'
  FROM xavefinance_ethereum.FXPoolFactory_call_newFXPool cc CROSS JOIN UNNEST(_assetsToRegister) AS t(token) WHERE call_success
  UNION ALL SELECT 'avalanche_c', cc.contract_address, cc.output_0, t.token, 0, cc._name, 'FX'
  FROM xavefinance_avalanche_c.FXPoolFactory_call_newFXPool cc CROSS JOIN UNNEST(_assetsToRegister) AS t(token) WHERE call_success
  UNION ALL SELECT 'polygon', cc.contract_address, cc.output_0, t.token, 0, cc._name, 'FX'
  FROM xavefinance_polygon.FXPoolFactory_call_newFXPool cc CROSS JOIN UNNEST(_assetsToRegister) AS t(token) WHERE call_success

  -- ═══════════════════════════════════════════
  -- MANAGED POOLS
  -- ═══════════════════════════════════════════

  UNION ALL SELECT 'avalanche_c', cc.contract_address, c.poolId, from_hex(t.tokens), 0, json_extract_scalar(params, '$.symbol'), 'managed'
  FROM balancer_v2_avalanche_c.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_avalanche_c.ManagedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_pool
  CROSS JOIN UNNEST(CAST(json_extract(settingsParams, '$.tokens') AS ARRAY(VARCHAR))) AS t(tokens)
  UNION ALL SELECT 'base', cc.contract_address, c.poolId, from_hex(t.tokens), 0, json_extract_scalar(params, '$.symbol'), 'managed'
  FROM balancer_v2_base.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_base.ManagedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_pool
  CROSS JOIN UNNEST(CAST(json_extract(settingsParams, '$.tokens') AS ARRAY(VARCHAR))) AS t(tokens)
  UNION ALL SELECT 'gnosis', cc.contract_address, c.poolId, from_hex(t.tokens), 0, json_extract_scalar(params, '$.symbol'), 'managed'
  FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_gnosis.ManagedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_pool
  CROSS JOIN UNNEST(CAST(json_extract(settingsParams, '$.tokens') AS ARRAY(VARCHAR))) AS t(tokens)
  UNION ALL SELECT 'zkevm', cc.contract_address, c.poolId, from_hex(t.tokens), 0, json_extract_scalar(params, '$.symbol'), 'managed'
  FROM balancer_v2_zkevm.Vault_evt_PoolRegistered c INNER JOIN balancer_v2_zkevm.ManagedPoolFactory_call_create cc ON c.evt_tx_hash = cc.call_tx_hash AND bytearray_substring(c.poolId,1,20) = cc.output_pool
  CROSS JOIN UNNEST(CAST(json_extract(settingsParams, '$.tokens') AS ARRAY(VARCHAR))) AS t(tokens)

),

-- ═══════════════════════════════════════════
-- ENRICH WITH TOKEN SYMBOLS
-- ═══════════════════════════════════════════

settings AS (
  SELECT
    p.blockchain,
    p.factory_address,
    p.pool_id,
    p.pool_type,
    p.symbol AS pool_symbol,
    COALESCE(t.symbol, '?') AS token_symbol,
    CAST(100 * p.normalized_weight AS integer) AS norm_weight
  FROM all_pools p
  LEFT JOIN tokens.erc20 t ON p.token_address = t.contract_address
),

-- ═══════════════════════════════════════════
-- BUILD POOL LABELS
-- ═══════════════════════════════════════════

labels AS (
  SELECT
    blockchain,
    factory_address,
    bytearray_substring(pool_id, 1, 20) AS address,
    pool_id,
    CASE
      WHEN pool_type IN ('stable', 'linear', 'LBP', 'ECLP', 'FX', 'managed')
        THEN lower(pool_symbol)
      ELSE lower(concat(
        array_join(array_agg(token_symbol ORDER BY token_symbol), '/'), ' ',
        array_join(array_agg(CAST(norm_weight AS varchar) ORDER BY token_symbol), '/')
      ))
    END AS name,
    pool_type
  FROM settings
  GROUP BY blockchain, factory_address, pool_id, pool_symbol, pool_type
)

-- ═══════════════════════════════════════════
-- FINAL OUTPUT
-- ═══════════════════════════════════════════

SELECT
  l.blockchain,
  l.factory_address,
  SUBSTRING(d.deployment_task, 10, 999) AS factory_version,
  l.pool_id,
  l.address AS pool_address,
  l.name AS pool_symbol,
  l.pool_type,
  q.pool_registered AS creation_date
FROM labels l
LEFT JOIN dune.balancer.dataset_balancer_deployments d
  ON l.factory_address = d.address
  AND d.blockchain = CASE l.blockchain
    WHEN 'ethereum'    THEN 'mainnet'
    WHEN 'avalanche_c' THEN 'avalanche'
    WHEN 'zkevm'       THEN 'zkevm'
    ELSE l.blockchain
  END
LEFT JOIN query_2634572 q
  ON q.blockchain = l.blockchain
  AND q.pool_address = l.address
ORDER BY l.blockchain