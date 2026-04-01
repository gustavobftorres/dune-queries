-- part of a query repo
-- query name: Balancer V2 LBP Stats (Dune SQL)
-- query link: https://dune.com/queries/2500058


WITH lbps AS (
        -- V2 LBPs
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as varchar) AS pool_id,
        'ethereum' as blockchain
    FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_ethereum.LiquidityBootstrappingPoolFactory_call_create cc 
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        'ethereum' as blockchain
    FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_ethereum.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as varchar) AS pool_id,
        'arbitrum' as blockchain
    FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_arbitrum.LiquidityBootstrappingPoolFactory_call_create cc 
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        'arbitrum' as blockchain
    FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_arbitrum.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        'avalanche_c' as blockchain
    FROM balancer_v2_avalanche_c.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_avalanche_c.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        'base' as blockchain
    FROM balancer_v2_base.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_base.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        'gnosis' as blockchain
    FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_gnosis.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        'optimism' as blockchain
    FROM balancer_v2_optimism.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_optimism.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as varchar) AS pool_id,
        'polygon' as blockchain
    FROM balancer_v2_polygon.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_polygon.LiquidityBootstrappingPoolFactory_call_create cc 
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success
    
    UNION ALL
    
    SELECT 
        (symbol) AS name,
        CAST ("poolId" as VARCHAR) AS pool_id,
        'polygon' as blockchain
    FROM balancer_v2_polygon.Vault_evt_PoolRegistered c
    INNER JOIN balancer_v2_polygon.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create cc
    ON CAST (c.evt_tx_hash as varchar) = CAST (cc.call_tx_hash as varchar)
    AND cc.call_success

    )
    
SELECT
    COUNT(DISTINCT t.tx_from) AS n_participants,
    COUNT(DISTINCT l.pool_id) AS n_lbps,
    SUM(t.amount_usd) AS volume,
    COUNT(*) AS n_transactions,
    MAX(t.block_time) - MIN(t.block_time) AS duration
FROM dex.trades t
INNER JOIN lbps l 
    ON SUBSTRING(l.pool_id, 1, 42) = CAST(t.project_contract_address AS VARCHAR) 
    AND l.blockchain = t.blockchain
WHERE t.project = 'balancer';