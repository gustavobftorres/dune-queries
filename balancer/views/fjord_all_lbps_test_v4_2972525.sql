-- part of a query repo
-- query name: fjord all lbps test v4
-- query link: https://dune.com/queries/2972525


WITH old_lbps AS ( 
    SELECT
        c.evt_block_number,
        c.evt_block_time,
        c.evt_tx_hash,
        c.poolId AS poolId,
        SUBSTRING(CAST(c.poolId AS VARCHAR), 1, 42) AS pool_address,
        cc.*
    FROM balancer_v2_ethereum.Vault_evt_PoolRegistered AS c
    INNER JOIN balancer_v2_ethereum.LiquidityBootstrappingPoolFactory_call_create AS cc
        ON c.evt_tx_hash = cc.call_tx_hash AND cc.call_success
), new_lbps AS (
  SELECT
        c.evt_block_number,
        c.evt_block_time,
        c.evt_tx_hash,
        c.poolId AS poolId,
        SUBSTRING(CAST(c.poolId AS VARCHAR), 1, 42) AS pool_address,
        cc.*
    FROM balancer_v2_ethereum.Vault_evt_PoolRegistered AS c
    INNER JOIN balancer_v2_ethereum.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create AS cc
        ON c.evt_tx_hash = cc.call_tx_hash AND cc.call_success
), old_new_lpbs AS (
  SELECT
    *
  FROM old_lbps
  UNION ALL
  SELECT
    *
  FROM new_lbps
)
SELECT
    name,
    contract_address,
    evt_tx_hash,
    evt_block_time,
    evt_block_number,
    owner,
    pool_address as pool,
    poolId,
    swapEnabledOnStart,
    swapFeePercentage,
    symbol,
    tokens,
    weights
    FROM old_new_lpbs