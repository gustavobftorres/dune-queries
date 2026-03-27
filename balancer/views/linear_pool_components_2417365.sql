-- part of a query repo
-- query name: Linear Pool Components
-- query link: https://dune.com/queries/2417365


WITH
    main_tokens_calls AS (
    --------------------------------------------------------------------------------------------------------------------------
    -- ARBITRUM LINEAR POOL MAIN TOKENS --------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    -- aave Linear Pools    
        SELECT *, 'arbitrum' AS blockchain, 'aave' AS lending_standard
        FROM balancer_v2_arbitrum.AaveLinearPool_call_getMainToken
        UNION
    -- erc4626 Linear Pools
        SELECT *, 'arbitrum' AS blockchain, 'erc4626' AS lending_standard
        FROM balancer_v2_arbitrum.ERC4626LinearPool_call_getMainToken
        UNION
    -- yearn Linear Pools
        SELECT *, 'arbitrum' AS blockchain, 'yearn' AS lending_standard
        FROM balancer_v2_arbitrum.YearnLinearPool_call_getMainToken
        UNION
    --------------------------------------------------------------------------------------------------------------------------
    -- ETHEREUM LINEAR POOL MAIN TOKENS --------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    -- aave Linear Pools    
        SELECT *, 'ethereum' AS blockchain, 'aave' AS lending_standard
        FROM balancer_v2_ethereum.AaveLinearPool_call_getMainToken
        UNION
    -- erc4626 Linear Pools
        SELECT *, 'ethereum' AS blockchain, 'erc4626' AS lending_standard
        FROM balancer_v2_ethereum.ERC4626LinearPool_call_getMainToken
        UNION
    -- euler Linear Pools
        SELECT *, 'ethereum' AS blockchain, 'euler' AS lending_standard
        FROM balancer_v2_ethereum.EulerLinearPool_call_getMainToken
    ---------------------------
    --    UNION
    ---------------------------
    -- THIS CONTRACT TYPE HAS YET TO BE DECODED BY DUNE
    -- gearbox Linear Pools
        -- SELECT *, 'ethereum' AS blockchain, 'gearbox' AS lending_standard
        -- FROM balancer_v2_ethereum.GearboxLinearPool_call_getMainToken
    ---------------------------
        UNION
    -- yearn Linear Pools
        SELECT *, 'ethereum' AS blockchain, 'yearn' AS lending_standard
        FROM balancer_v2_ethereum.YearnLinearPool_call_getMainToken
        UNION
    --------------------------------------------------------------------------------------------------------------------------
    -- GNOSIS LINEAR POOL MAIN TOKENS ----------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    -- aave Linear Pools    
        SELECT *, 'gnosis' AS blockchain, 'aave' AS lending_standard
        FROM balancer_v2_gnosis.AaveLinearPool_call_getMainToken  
        UNION
    --------------------------------------------------------------------------------------------------------------------------
    -- POLYGON LINEAR POOL MAIN TOKENS ---------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    -- aave Linear Pools    
        SELECT *, 'polygon' AS blockchain, 'aave' AS lending_standard
        FROM balancer_v2_polygon.AaveLinearPool_call_getMainToken
        UNION
    -- erc4626 Linear Pools
        SELECT *, 'polygon' AS blockchain, 'erc4626' AS lending_standard
        FROM balancer_v2_polygon.ERC4626LinearPool_call_getMainToken
        UNION
    -- yearn Linear Pools
        SELECT *, 'polygon' AS blockchain, 'yearn' AS lending_standard
        FROM balancer_v2_polygon.YearnLinearPool_call_getMainToken
    ),
    
    --------------------------------------------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    
    wrapped_tokens_calls AS (
    --------------------------------------------------------------------------------------------------------------------------
    -- ARBITRUM LINEAR POOL WRAPPED TOKENS -----------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    -- aave Linear Pools    
        SELECT *, 'arbitrum' AS blockchain, 'aave' AS lending_standard
        FROM balancer_v2_arbitrum.AaveLinearPool_call_getWrappedToken
        UNION
    -- erc4626 Linear Pools
        SELECT *, 'arbitrum' AS blockchain, 'erc4626' AS lending_standard
        FROM balancer_v2_arbitrum.ERC4626LinearPool_call_getWrappedToken
        UNION
    -- yearn Linear Pools
        SELECT *, 'arbitrum' AS blockchain, 'yearn' AS lending_standard
        FROM balancer_v2_arbitrum.YearnLinearPool_call_getWrappedToken
        UNION
    --------------------------------------------------------------------------------------------------------------------------
    -- ETHEREUM LINEAR POOL WRAPPED TOKENS -----------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    -- aave Linear Pools    
        SELECT *, 'ethereum' AS blockchain, 'aave' AS lending_standard
        FROM balancer_v2_ethereum.AaveLinearPool_call_getWrappedToken
        UNION
    -- erc4626 Linear Pools
        SELECT *, 'ethereum' AS blockchain, 'erc4626' AS lending_standard
        FROM balancer_v2_ethereum.ERC4626LinearPool_call_getWrappedToken
        UNION
    -- euler Linear Pools
        SELECT *, 'ethereum' AS blockchain, 'euler' AS lending_standard
        FROM balancer_v2_ethereum.EulerLinearPool_call_getWrappedToken
    ---------------------------
    --    UNION
    ---------------------------
    -- THIS CONTRACT TYPE HAS YET TO BE DECODED BY DUNE
    -- gearbox Linear Pools
        -- SELECT *, 'ethereum' AS blockchain, 'gearbox' AS lending_standard
        -- FROM balancer_v2_ethereum.GearboxLinearPool_call_getWrappedToken
    ---------------------------
        UNION
    -- yearn Linear Pools
        SELECT *, 'ethereum' AS blockchain, 'yearn' AS lending_standard
        FROM balancer_v2_ethereum.YearnLinearPool_call_getWrappedToken
        UNION
    --------------------------------------------------------------------------------------------------------------------------
    -- GNOSIS LINEAR POOL WRAPPED TOKENS -------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    -- aave Linear Pools    
        SELECT *, 'gnosis' AS blockchain, 'aave' AS lending_standard
        FROM balancer_v2_gnosis.AaveLinearPool_call_getWrappedToken  
        UNION
    --------------------------------------------------------------------------------------------------------------------------
    -- POLYGON LINEAR POOL WRAPPED TOKENS ------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    -- aave Linear Pools    
        SELECT *, 'polygon' AS blockchain, 'aave' AS lending_standard
        FROM balancer_v2_polygon.AaveLinearPool_call_getWrappedToken
        UNION
    -- erc4626 Linear Pools
        SELECT *, 'polygon' AS blockchain, 'erc4626' AS lending_standard
        FROM balancer_v2_polygon.ERC4626LinearPool_call_getWrappedToken
    --     UNION
    -- -- yearn Linear Pools
    --     SELECT *, 'polygon' AS blockchain, 'yearn' AS lending_standard
    --     FROM balancer_v2_polygon.YearnLinearPool_call_getWrapped
    ),
    
    --------------------------------------------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------    

    
    main_tokens AS (SELECT distinct blockchain, contract_address, lending_standard, output_0 AS main_token FROM main_tokens_calls),
    wrapped_tokens AS (SELECT distinct blockchain, contract_address, lending_standard, output_0 AS wrapped_token FROM wrapped_tokens_calls),
    
    main_and_wrapped_tokens AS (
        SELECT 
            m.blockchain, 
            m.lending_standard, 
            m.contract_address AS pool_token,
            m.main_token, 
            w.wrapped_token 
        FROM main_tokens m 
        LEFT JOIN wrapped_tokens w ON m.contract_address = w.contract_address AND m.blockchain = w.blockchain
    )

SELECT * FROM main_and_wrapped_tokens
    
    