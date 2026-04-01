-- part of a query repo
-- query name: (query_2511450) balancer_v2_lbps
-- query link: https://dune.com/queries/2511450


/*
queried on:
Balancer V2 LBP Stats (Dune SQL) https://dune.com/queries/2500058
Balancer V2 LBP Sales Stats (Dune SQL) https://dune.com/queries/2836761
Balancer V2 LBP Balances (Dune SQL) https://dune.com/queries/2836792
Balancer V2 LBP, Hourly (Dune SQL) https://dune.com/queries/2836798
Balancer V2 LBP Indirect Volume (Dune SQL) https://dune.com/queries/2836807
Balancer V2 LBP Token Holders (Dune SQL )https://dune.com/queries/2510848
Balancer V2 Pool Token Price (Dune SQL) https://dune.com/queries/2510861
Balancer V2 Token Price Stats (Dune SQL) https://dune.com/queries/2510878
Balancer LBPs Ranking https://dune.com/queries/226248
Balancer LBPs Total Funds Raised https://dune.com/queries/226267
*/

WITH lbps_call_create AS (
        SELECT tokens, symbol, call_tx_hash, call_success, 'ethereum' as blockchain FROM balancer_v2_ethereum.LiquidityBootstrappingPoolFactory_call_create
        UNION ALL
        SELECT tokens, symbol, call_tx_hash, call_success, 'ethereum' as blockchain FROM balancer_v2_ethereum.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create
        UNION ALL
        SELECT tokens, symbol, call_tx_hash, call_success, 'arbitrum' as blockchain FROM balancer_v2_arbitrum.LiquidityBootstrappingPoolFactory_call_create
        UNION ALL
        SELECT tokens, symbol, call_tx_hash, call_success, 'arbitrum' as blockchain FROM balancer_v2_arbitrum.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create
        UNION ALL
        SELECT tokens, symbol, call_tx_hash, call_success, 'avalanche_c' as blockchain FROM balancer_v2_avalanche_c.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create
        UNION ALL
        SELECT tokens, symbol, call_tx_hash, call_success, 'base' as blockchain FROM balancer_v2_base.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create
        UNION ALL
        SELECT tokens, symbol, call_tx_hash, call_success, 'gnosis' as blockchain FROM balancer_v2_gnosis.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create
        UNION ALL
        SELECT tokens, symbol, call_tx_hash, call_success, 'optimism' as blockchain FROM balancer_v2_optimism.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create
        UNION ALL
        SELECT tokens, symbol, call_tx_hash, call_success, 'polygon' as blockchain FROM balancer_v2_polygon.LiquidityBootstrappingPoolFactory_call_create
        UNION ALL
        SELECT tokens, symbol, call_tx_hash, call_success, 'polygon' as blockchain FROM balancer_v2_polygon.NoProtocolFeeLiquidityBootstrappingPoolFactory_call_create
    ),

    lbps_registered AS(
    SELECT poolId, evt_tx_hash, 'ethereum' as blockchain FROM balancer_v2_ethereum.Vault_evt_PoolRegistered c
    UNION ALL
    SELECT poolId, evt_tx_hash, 'arbitrum' as blockchain FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered c
    UNION ALL
    SELECT poolId, evt_tx_hash, 'avalanche_c' as blockchain FROM balancer_v2_avalanche_c.Vault_evt_PoolRegistered c
    UNION ALL
    SELECT poolId, evt_tx_hash, 'base' as blockchain FROM balancer_v2_base.Vault_evt_PoolRegistered c
    UNION ALL
    SELECT poolId, evt_tx_hash, 'gnosis' as blockchain FROM balancer_v2_gnosis.Vault_evt_PoolRegistered c
    UNION ALL
    SELECT poolId, evt_tx_hash, 'optimism' as blockchain FROM balancer_v2_optimism.Vault_evt_PoolRegistered c
    UNION ALL
    SELECT poolId, evt_tx_hash, 'polygon' as blockchain FROM balancer_v2_polygon.Vault_evt_PoolRegistered c
    ),    

    lbps_list AS (
        SELECT 
            tokens,
            lower(symbol) AS name,
            CAST("poolId" as varchar) AS pool_id,
            c.blockchain,
            SUBSTRING(CAST("poolId" as varchar), 1, 42) AS pool_address
        FROM lbps_registered c
        INNER JOIN lbps_call_create cc
        ON c.evt_tx_hash = cc.call_tx_hash AND c.blockchain = cc.blockchain
        AND cc.call_success
    ),
    
    lbps_weight_update AS (
        SELECT *, 'ethereum' as blockchain
        FROM balancer_v2_ethereum.LiquidityBootstrappingPool_evt_GradualWeightUpdateScheduled
        UNION ALL
        SELECT *, 'ethereum' as blockchain
        FROM balancer_v2_ethereum.NoProtocolFeeLiquidityBootstrappingPool_evt_GradualWeightUpdateScheduled
        UNION ALL
        SELECT *, 'polygon' as blockchain
        FROM balancer_v2_polygon.NoProtocolFeeLiquidityBootstrappingPool_evt_GradualWeightUpdateScheduled
    ),

    last_weight_update AS (
        SELECT *
        FROM (
            SELECT 
                CAST(contract_address as varchar) AS pool_address,
                blockchain,
                from_unixtime(CAST("startTime" AS bigint)) AS start_time,
                from_unixtime(CAST("endTime" AS bigint)) AS end_time,
                startWeights as start_weights,
                ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY evt_block_time DESC) AS ranking
            FROM lbps_weight_update c
        ) w
        WHERE ranking = 1
    ),
    
    lbps_tokens_weights AS (
        SELECT 
            name,
            pool_id,
            l.pool_address,
            start_time,
            end_time,
            token,
            start_weight
        FROM lbps_list l
        LEFT JOIN last_weight_update w
        ON w.pool_address = l.pool_address AND w.blockchain = l.blockchain
        CROSS JOIN UNNEST(tokens) AS t(token)
        CROSS JOIN UNNEST(start_weights) AS t(start_weight)
    ),
    
    lbps_info AS (
        SELECT 
            *
        FROM (
            SELECT 
                *,
               ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY start_weight DESC) AS ranking 
            FROM lbps_tokens_weights
            WHERE CAST(token as varchar) NOT IN (
                CAST('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as VARCHAR), -- WETH
                CAST('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' as varchar), -- USDC
                CAST('0x6b175474e89094c44da98b954eedeac495271d0f' as varchar), -- DAI
                CAST('0x88acdd2a6425c3faae4bc9650fd7e27e0bebb7ab' as varchar), -- MIST
                CAST('0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5' as varchar) -- OHM
            )
        ) l
        WHERE ranking = 1
    )

SELECT 
    l.name,
    SUBSTRING(pool_id,1,42) as pool_id,
    blockchain,
    token AS token_sold,
    t.symbol AS token_symbol,
    start_time,
    COALESCE(end_time, TIMESTAMP '2999-01-01') AS end_time
FROM lbps_info l
LEFT JOIN tokens.erc20 t
ON l.token = t.contract_address