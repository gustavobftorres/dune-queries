-- part of a query repo
-- query name: Factory <> Pool Mapping (arb)
-- query link: https://dune.com/queries/4080520


WITH pools AS (
  SELECT
    factory_address,
    pool_id,
    zip.tokens AS token_address,
    zip.weights / pow(10, 18) AS normalized_weight,
    symbol,
    pool_type
  FROM (
    SELECT
      cc.contract_address AS factory_address,
      c.poolId AS pool_id,
      t.tokens,
      w.weights,
      cc.symbol,
      'weighted' AS pool_type
    FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_arbitrum.WeightedPoolFactory_call_create cc
      ON c.evt_tx_hash = cc.call_tx_hash
      AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
    CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
    CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos)
    WHERE t.pos = w.pos
  ) zip

  UNION ALL

  SELECT
    factory_address,
    pool_id,
    zip.tokens AS token_address,
    zip.weights / pow(10, 18) AS normalized_weight,
    symbol,
    pool_type
  FROM (
    SELECT
      cc.contract_address AS factory_address,
      c.poolId AS pool_id,
      t.tokens,
      w.weights,
      cc.symbol,
      'weighted' AS pool_type
    FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_arbitrum.WeightedPoolFactory_call_create cc
      ON c.evt_tx_hash = cc.call_tx_hash
      AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
    CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
    CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos)
    WHERE t.pos = w.pos
  ) zip

  UNION ALL

  SELECT
    factory_address,
    pool_id,
    zip.tokens AS token_address,
    zip.weights / pow(10, 18) AS normalized_weight,
    symbol,
    pool_type
  FROM (
    SELECT
      cc.contract_address AS factory_address,
      c.poolId AS pool_id,
      t.tokens,
      w.weights,
      cc.symbol,
      'weighted' AS pool_type
    FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_arbitrum.WeightedPoolV2Factory_call_create cc
      ON c.evt_tx_hash = cc.call_tx_hash
      AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
    CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
    CROSS JOIN UNNEST(cc.normalizedWeights) WITH ORDINALITY w(weights, pos)
    WHERE t.pos = w.pos
  ) zip

  UNION ALL

  SELECT
    factory_address,
    pool_id,
    zip.tokens AS token_address,
    zip.weights / pow(10, 18) AS normalized_weight,
    symbol,
    pool_type
  FROM (
    SELECT
      cc.contract_address AS factory_address,
      c.poolId AS pool_id,
      t.tokens,
      w.weights,
      cc.symbol,
      'investment' AS pool_type
    FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_arbitrum.InvestmentPoolFactory_call_create cc
      ON c.evt_tx_hash = cc.call_tx_hash
      AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
    CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
    CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos)
    WHERE t.pos = w.pos
  ) zip

  UNION ALL

  SELECT
    factory_address,
    pool_id,
    zip.tokens AS token_address,
    zip.weights / pow(10, 18) AS normalized_weight,
    symbol,
    pool_type
  FROM (
    SELECT
      cc.contract_address AS factory_address,
      c.poolId AS pool_id,
      t.tokens,
      w.weights,
      cc.symbol,
      'weighted' AS pool_type
    FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_arbitrum.WeightedPool2TokensFactory_call_create cc
      ON c.evt_tx_hash = cc.call_tx_hash
      AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
    CROSS JOIN UNNEST(cc.tokens) WITH ORDINALITY t(tokens, pos)
    CROSS JOIN UNNEST(cc.weights) WITH ORDINALITY w(weights, pos)
    WHERE t.pos = w.pos
  ) zip

  UNION ALL

  SELECT  cc.contract_address AS factory_address,
    c.poolId AS pool_id,
    t.tokens,
    0 AS weights,
    cc.symbol,
    'stable' AS pool_type
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_arbitrum.StablePoolFactory_call_create cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL

  SELECT  cc.contract_address AS factory_address,
    c.poolId AS pool_id,
    t.tokens AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'stable' AS pool_type
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_arbitrum.MetaStablePoolFactory_call_create cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL

  SELECT  cc.contract_address AS factory_address,
    c.poolId AS pool_id,
    t.tokens AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'LBP' AS pool_type
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_arbitrum.LiquidityBootstrappingPoolFactory_call_create cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL

  SELECT  cc.contract_address AS factory_address,
    c.poolId AS pool_id,
    t.tokens AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'LBP' AS pool_type
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_arbitrum.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL

  SELECT  cc.contract_address AS factory_address,
    c.poolId AS pool_id,
    t.tokens AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'stable' AS pool_type
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_arbitrum.ComposableStablePoolFactory_call_create cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL

  SELECT  cc.contract_address AS factory_address,
    c.poolId AS pool_id,
    element AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'linear' AS pool_type
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_arbitrum.AaveLinearPoolFactory_call_create cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t (element)

  UNION ALL

  SELECT  cc.contract_address AS factory_address,
    c.poolId AS pool_id,
    element AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'linear' AS pool_type
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN balancer_v2_arbitrum.ERC4626LinearPoolFactory_call_create cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(ARRAY[cc.mainToken, cc.wrappedToken]) AS t (element)

  UNION ALL

  SELECT  cc.contract_address AS factory_address,
    c.poolId AS pool_id,
    t.tokens AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'ECLP' AS pool_type
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN gyroscope_arbitrum.GyroECLPPoolFactory_call_create cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)

  UNION ALL

  SELECT  cc.contract_address AS factory_address,
    c.poolId AS pool_id,
    t.tokens AS token_address,
    0 AS normalized_weight,
    cc.symbol,
    'ECLP' AS pool_type
  FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
  INNER JOIN gyroscope_arbitrum.Gyro2CLPPoolFactory_call_create cc
    ON c.evt_tx_hash = cc.call_tx_hash
    AND bytearray_substring(c.poolId, 1, 20) = cc.output_0
  CROSS JOIN UNNEST(cc.tokens) AS t(tokens)
),

settings AS (
  SELECT
    factory_address,
    pool_id,
    coalesce(t.symbol, '?') AS token_symbol,
    normalized_weight,
    p.symbol AS pool_symbol,
    p.pool_type
  FROM pools p
  LEFT JOIN tokens.erc20 t ON p.token_address = t.contract_address
), 

labels AS(
SELECT 
  'arbitrum' AS blockchain,
  factory_address,
  bytearray_substring(pool_id, 1, 20) AS address,
  CASE WHEN pool_type IN ('stable', 'linear', 'LBP', 'ECLP', 'FX') 
  THEN lower(pool_symbol)
    ELSE lower(concat(array_join(array_agg(token_symbol ORDER BY token_symbol), '/'), ' ', 
    array_join(array_agg(cast(norm_weight AS varchar) ORDER BY token_symbol), '/')))
  END AS name,
  pool_type
FROM (
  SELECT
  factory_address,
    s1.pool_id,
    token_symbol,
    pool_symbol,
    cast(100 * normalized_weight AS integer) AS norm_weight,
    pool_type
  FROM settings s1
  GROUP BY factory_address, s1.pool_id, token_symbol, pool_symbol, normalized_weight, pool_type
) s
GROUP BY factory_address, pool_id, pool_symbol, pool_type
ORDER BY 1)

SELECT 
    l.blockchain,
    l.factory_address,
    SUBSTRING(d.deployment_task, 10,999) AS factory_version,
    l.address AS pool_address,
    l.name AS pool_symbol,
    l.pool_type
FROM labels l
LEFT JOIN dune.balancer.dataset_balancer_deployments d ON l.factory_address = d.address
AND d.blockchain = 'arbitrum'
