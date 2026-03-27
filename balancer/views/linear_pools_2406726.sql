-- part of a query repo
-- query name: Linear Pools
-- query link: https://dune.com/queries/2406726


WITH
    linear_pool_created AS (
        -- Actually the "LenderName"LinearPoolCreate event marks the compleation of the creation of a linear pool in version 4.
        -- The event PoolCreated marks the first instanitation of the Linear Pool. Both of these events occur in the current
        -- Linear Pool smart contract version 4. Previously only PoolCreated occurred.
        
        -- CURRENT LINEAR POOL FACTORY EVENTS
        -- Linear Pool Factory evt = "LinearPoolCreated"
        -- This is the name of the event where a linear pool is created in version 4
        
        -- OLD LINEAR POOL FACTORY EVENTS
        -- Keeping these queries in here for the time being due to the high value of the old bb-a-usd linear pools.
        -- Linear Pool Factory evt = "PoolCreated"
        -- This is the name of the event where a linear pool is created in versions < 4
    
        --------------------------------------------------------------------------------------------------------------------------
        -- ARBITRUM LINEAR POOLS -------------------------------------------------------------------------------------------------
        --------------------------------------------------------------------------------------------------------------------------
        -- CURRENT LINEAR POOL FACTORY EVENTS
        SELECT *, 'arbitrum' AS blockchain, 'aave' AS lending_standard FROM balancer_v2_arbitrum.AaveLinearPoolFactory_evt_AaveLinearPoolCreated
        UNION
        SELECT *, 'arbitrum' AS blockchain, 'erc4626' AS lending_standard FROM balancer_v2_arbitrum.ERC4626LinearPoolFactory_evt_Erc4626LinearPoolCreated
        UNION
        SELECT *, 'arbitrum' AS blockchain, 'yearn' AS lending_standard FROM balancer_v2_arbitrum.YearnLinearPoolFactory_evt_YearnLinearPoolCreated
        UNION
        --------------------------------------------------------------------------------------------------------------------------
        -- ETHEREUM LINEAR POOLS -------------------------------------------------------------------------------------------------
        --------------------------------------------------------------------------------------------------------------------------
        -- CURRENT LINEAR POOL FACTORY EVENTS
        SELECT *, 'ethereum' AS blockchain, 'aave' AS lending_standard FROM balancer_v2_ethereum.AaveLinearPoolFactory_evt_AaveLinearPoolCreated
        UNION
        SELECT *, 'ethereum' AS blockchain, 'erc4626' AS lending_standard FROM balancer_v2_ethereum.ERC4626LinearPoolFactory_evt_Erc4626LinearPoolCreated
        UNION
        SELECT *, 'ethereum' AS blockchain, 'euler' AS lending_standard FROM balancer_v2_ethereum.EulerLinearPoolFactory_evt_EulerLinearPoolCreated
        UNION
        SELECT *, 'ethereum' AS blockchain, 'gearbox' AS lending_standard FROM balancer_v2_ethereum.GearboxLinearPoolFactory_evt_GearboxLinearPoolCreated
        UNION
        SELECT *, 'ethereum' AS blockchain, 'yearn' AS lending_standard FROM balancer_v2_ethereum.YearnLinearPoolFactory_evt_YearnLinearPoolCreated
        UNION
        -- Adding this in to get the old bb-a-usd pool
        SELECT *, NULL AS protocolID, 'ethereum' AS blockchain, 'aave' AS lending_standard FROM balancer_v2_ethereum.AaveLinearPoolFactory_evt_PoolCreated
        UNION
        --------------------------------------------------------------------------------------------------------------------------
        -- GNOSIS LINEAR POOLS ---------------------------------------------------------------------------------------------------
        --------------------------------------------------------------------------------------------------------------------------
        -- CURRENT LINEAR POOL FACTORY EVENTS
        SELECT *, 'gnosis' AS blockchain, 'aave' AS lending_standard FROM balancer_v2_gnosis.AaveLinearPoolFactory_evt_AaveLinearPoolCreated
        UNION
        -- Below is the Aave V3 Factory. Adding for good record keeping but this Factory was only used as a test on Gnosis.
        SELECT *, 'gnosis' AS blockchain, 'aave' AS lending_standard FROM balancer_v2_gnosis.AaveLinearPoolV3Factory_evt_AaveLinearPoolCreated
        UNION
        --------------------------------------------------------------------------------------------------------------------------
        -- POLYGON LINEAR POOLS --------------------------------------------------------------------------------------------------
        --------------------------------------------------------------------------------------------------------------------------
        -- CURRENT LINEAR POOL FACTORY EVENTS
        SELECT *, 'polygon' AS blockchain, 'aave' AS lending_standard FROM balancer_v2_polygon.AaveLinearPoolFactory_evt_AaveLinearPoolCreated
        UNION
        SELECT *, 'polygon' AS blockchain, 'erc4626' AS lending_standard FROM balancer_v2_polygon.ERC4626LinearPoolFactory_evt_Erc4626LinearPoolCreated
        UNION
        SELECT *, 'polygon' AS blockchain, 'yearn' AS lending_standard FROM balancer_v2_polygon.YearnLinearPoolFactory_evt_YearnLinearPoolCreated
    ),
    
    all_linear_pool_ids AS ( 
        --------------------------------------------------------------------------------------------------------------------------
        -- ARBITRUM LINEAR POOL IDS ----------------------------------------------------------------------------------------------
        --------------------------------------------------------------------------------------------------------------------------
        SELECT *, 'arbitrum' AS blockchain, 'aave' AS lending_standard FROM balancer_v2_arbitrum.AaveLinearPool_call_getPoolId
        UNION
        SELECT *, 'arbitrum' AS blockchain, 'erc4626' AS lending_standard FROM balancer_v2_arbitrum.ERC4626LinearPool_call_getPoolId
        UNION
        SELECT *, 'arbitrum' AS blockchain, 'yearn' AS lending_standard FROM balancer_v2_arbitrum.YearnLinearPool_call_getPoolId
        UNION
        --------------------------------------------------------------------------------------------------------------------------
        -- ETHEREUM LINEAR POOL IDS -------------------------------------------------------------------------------------------------
        --------------------------------------------------------------------------------------------------------------------------
        SELECT *, 'ethereum' AS blockchain, 'aave' AS lending_standard FROM balancer_v2_ethereum.AaveLinearPool_call_getPoolId
        UNION
        SELECT *, 'ethereum' AS blockchain, 'erc4626' AS lending_standard FROM balancer_v2_ethereum.ERC4626LinearPool_call_getPoolId
        UNION
        SELECT *, 'ethereum' AS blockchain, 'euler' AS lending_standard FROM balancer_v2_ethereum.EulerLinearPool_call_getPoolId
        UNION
        -- SELECT *, 'ethereum' AS blockchain, 'gearbox' AS lending_standard FROM balancer_v2_ethereum.GearboxLinearPool_call_getPoolId
        -- UNION
        SELECT *, 'ethereum' AS blockchain, 'yearn' AS lending_standard FROM balancer_v2_ethereum.YearnLinearPool_call_getPoolId
        UNION
        --------------------------------------------------------------------------------------------------------------------------
        -- GNOSIS LINEAR POOL IDS ------------------------------------------------------------------------------------------------
        --------------------------------------------------------------------------------------------------------------------------
        SELECT *, 'gnosis' AS blockchain, 'aave' AS lending_standard FROM balancer_v2_gnosis.AaveLinearPool_call_getPoolId
        UNION
        -- Below is the Aave V3 Factory. Adding for good record keeping but this Factory was only used as a test on Gnosis.
        SELECT *, 'gnosis' AS blockchain, 'aave' AS lending_standard FROM balancer_v2_gnosis.AaveLinearPool_call_getPoolId
        UNION
        --------------------------------------------------------------------------------------------------------------------------
        -- POLYGON LINEAR POOL IDS -----------------------------------------------------------------------------------------------
        --------------------------------------------------------------------------------------------------------------------------
        SELECT *, 'polygon' AS blockchain, 'aave' AS lending_standard FROM balancer_v2_polygon.AaveLinearPool_call_getPoolId
        UNION
        SELECT *, 'polygon' AS blockchain, 'erc4626' AS lending_standard FROM balancer_v2_polygon.ERC4626LinearPool_call_getPoolId
        UNION
        SELECT *, 'polygon' AS blockchain, 'yearn' AS lending_standard FROM balancer_v2_polygon.YearnLinearPool_call_getPoolId
    ),
    
    linear_pool_ids AS (SELECT distinct contract_address AS pool_token, output_0 AS poolID, blockchain FROM all_linear_pool_ids)
    
SELECT 
    --evt_block_time, 
    --evt_block_number, 
    pc.blockchain, 
    pc.lending_standard, 
    pc.contract_address AS factory_address, 
    pc.pool,
    id.poolID,
    --evt_tx_hash, 
    --evt_index, 
    pc.protocolId 
FROM linear_pool_created pc
LEFT JOIN linear_pool_ids id ON (pc.pool = id.pool_token AND pc.blockchain = id.blockchain)
