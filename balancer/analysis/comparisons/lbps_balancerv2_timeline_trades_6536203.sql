-- part of a query repo
-- query name: LBPs | BalancerV2 | timeline_trades
-- query link: https://dune.com/queries/6536203


/* BALANCER V2 MULTICHAIN TRADES
   Purpose: Get granular trade history to calculate Drawdown & Buy Pressure
*/

WITH target_pools AS (
    -- 1. FILTER: Only get trades for the LBPs we identified (Optimization)
    -- We paste the logic to identify LBP addresses quickly
    SELECT pool as pool_address, 'ethereum' as blockchain FROM balancer_v2_ethereum.LiquidityBootstrappingPoolFactory_evt_PoolCreated
    UNION ALL SELECT pool, 'ethereum' FROM balancer_v2_ethereum.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
    UNION ALL SELECT pool, 'polygon' FROM balancer_v2_polygon.LiquidityBootstrappingPoolFactory_evt_PoolCreated
    UNION ALL SELECT pool, 'polygon' FROM balancer_v2_polygon.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
    UNION ALL SELECT pool, 'arbitrum' FROM balancer_v2_arbitrum.LiquidityBootstrappingPoolFactory_evt_PoolCreated
    UNION ALL SELECT pool, 'arbitrum' FROM balancer_v2_arbitrum.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
    UNION ALL SELECT pool, 'optimism' FROM balancer_v2_optimism.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
    UNION ALL SELECT pool, 'base' FROM balancer_v2_base.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
    UNION ALL SELECT pool, 'gnosis' FROM balancer_v2_gnosis.NoProtocolFeeLiquidityBootstrappingPoolFactory_evt_PoolCreated
),

formatted_pools AS (
    SELECT 
        pool_address,
        CAST(pool_address AS varbinary) as pool_address_bytes,
        blockchain
    FROM target_pools
)

-- 2. FETCH TRADES
SELECT 
    t.project_contract_address AS pool_id_bytes, -- Used to map back in Python
    t.blockchain,
    t.block_time,
    t.tx_hash,
    t.amount_usd,
    t.token_bought_amount,
    
    -- Price Calculation (USD per Token)
    CASE 
        WHEN t.token_bought_amount > 0 THEN t.amount_usd / t.token_bought_amount 
        ELSE 0 
    END AS price,
    
    t.token_bought_address

FROM dex.trades t
JOIN formatted_pools p 
  ON p.pool_address_bytes = t.project_contract_address 
  AND p.blockchain = t.blockchain
WHERE t.project = 'balancer' 
  AND t.version = '2'
  AND t.amount_usd > 0
  -- Optimization: Limit to last 2 years if query times out, but try full history first
  AND t.block_time > DATE('2021-01-01')

ORDER BY t.block_time ASC