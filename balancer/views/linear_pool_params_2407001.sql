-- part of a query repo
-- query name: Linear Pool Params
-- query link: https://dune.com/queries/2407001


WITH 
    current_swapFee AS (
        WITH
            all_swapFee AS (
            --------------------------------------------------------------------------------------------------------------------------
            -- ARBITRUM LINEAR POOL RESERVE TARGETS ----------------------------------------------------------------------------------
            --------------------------------------------------------------------------------------------------------------------------
            -- aave Linear Pools    
                SELECT *, 'arbitrum' AS blockchain, 'aave' AS lending_standard
                FROM balancer_v2_arbitrum.AaveLinearPool_evt_SwapFeePercentageChanged
                UNION
            -- erc4626 Linear Pools
                SELECT *, 'arbitrum' AS blockchain, 'erc4626' AS lending_standard
                FROM balancer_v2_arbitrum.ERC4626LinearPool_evt_SwapFeePercentageChanged
                UNION
            -- yearn Linear Pools
                SELECT *, 'arbitrum' AS blockchain, 'yearn' AS lending_standard
                FROM balancer_v2_arbitrum.YearnLinearPool_evt_SwapFeePercentageChanged
                UNION
            --------------------------------------------------------------------------------------------------------------------------
            -- ETHEREUM LINEAR POOL RESERVE TARGETS ----------------------------------------------------------------------------------
            --------------------------------------------------------------------------------------------------------------------------
            -- aave Linear Pools    
                SELECT *, 'ethereum' AS blockchain, 'aave' AS lending_standard
                FROM balancer_v2_ethereum.AaveLinearPool_evt_SwapFeePercentageChanged
                UNION
            -- erc4626 Linear Pools
                SELECT *, 'ethereum' AS blockchain, 'erc4626' AS lending_standard
                FROM balancer_v2_ethereum.ERC4626LinearPool_evt_SwapFeePercentageChanged
                UNION
            -- euler Linear Pools
                SELECT *, 'ethereum' AS blockchain, 'euler' AS lending_standard
                FROM balancer_v2_ethereum.EulerLinearPool_evt_SwapFeePercentageChanged
            ---------------------------
            --    UNION
            ---------------------------
            -- THIS CONTRACT TYPE HAS YET TO BE DECODED BY DUNE
            -- gearbox Linear Pools
                -- SELECT *, 'ethereum' AS blockchain, 'gearbox' AS lending_standard
                -- FROM balancer_v2_ethereum.GearboxLinearPool_evt_SwapFeePercentageChanged
            ---------------------------
                UNION
            -- yearn Linear Pools
                SELECT *, 'ethereum' AS blockchain, 'yearn' AS lending_standard
                FROM balancer_v2_ethereum.YearnLinearPool_evt_SwapFeePercentageChanged
                UNION
            --------------------------------------------------------------------------------------------------------------------------
            -- GNOSIS LINEAR POOL RESERVE TARGETS ------------------------------------------------------------------------------------
            --------------------------------------------------------------------------------------------------------------------------
            -- aave Linear Pools    
                SELECT contract_address, evt_tx_hash, evt_index, evt_block_time, evt_block_number, swapFeePercentage, 'gnosis' AS blockchain, 'aave' AS lending_standard
                FROM balancer_v2_gnosis.AaveLinearPool_evt_SwapFeePercentageChanged  
                UNION
            --------------------------------------------------------------------------------------------------------------------------
            -- POLYGON LINEAR POOL RESERVE TARGETS -----------------------------------------------------------------------------------
            --------------------------------------------------------------------------------------------------------------------------
            -- aave Linear Pools    
                SELECT *, 'polygon' AS blockchain, 'aave' AS lending_standard
                FROM balancer_v2_polygon.AaveLinearPool_evt_SwapFeePercentageChanged
                UNION
            -- erc4626 Linear Pools
                SELECT *, 'polygon' AS blockchain, 'erc4626' AS lending_standard
                FROM balancer_v2_polygon.ERC4626LinearPool_evt_SwapFeePercentageChanged
                UNION
            -- yearn Linear Pools
                SELECT *, 'polygon' AS blockchain, 'yearn' AS lending_standard
                FROM balancer_v2_polygon.YearnLinearPool_evt_SwapFeePercentageChanged
        )

        SELECT * FROM (
            SELECT *, 
                ROW_NUMBER() OVER(
                    PARTITION BY blockchain, contract_address 
                    ORDER BY blockchain ASC, evt_block_time DESC, evt_index DESC
                    ) AS latest_update 
            FROM all_swapFee
        )
        WHERE latest_update = 1
    ),
    
    current_targets AS (
        WITH
            all_targets AS (
            --------------------------------------------------------------------------------------------------------------------------
            -- ARBITRUM LINEAR POOL RESERVE TARGETS ----------------------------------------------------------------------------------
            --------------------------------------------------------------------------------------------------------------------------
            -- aave Linear Pools    
                SELECT *, 'arbitrum' AS blockchain, 'aave' AS lending_standard
                FROM balancer_v2_arbitrum.AaveLinearPool_evt_TargetsSet
                UNION
            -- erc4626 Linear Pools
                SELECT *, 'arbitrum' AS blockchain, 'erc4626' AS lending_standard
                FROM balancer_v2_arbitrum.ERC4626LinearPool_evt_TargetsSet
                UNION
            -- yearn Linear Pools
                SELECT *, 'arbitrum' AS blockchain, 'yearn' AS lending_standard
                FROM balancer_v2_arbitrum.YearnLinearPool_evt_TargetsSet
                UNION
            --------------------------------------------------------------------------------------------------------------------------
            -- ETHEREUM LINEAR POOL RESERVE TARGETS ----------------------------------------------------------------------------------
            --------------------------------------------------------------------------------------------------------------------------
            -- aave Linear Pools    
                SELECT *, 'ethereum' AS blockchain, 'aave' AS lending_standard
                FROM balancer_v2_ethereum.AaveLinearPool_evt_TargetsSet
                UNION
            -- erc4626 Linear Pools
                SELECT *, 'ethereum' AS blockchain, 'erc4626' AS lending_standard
                FROM balancer_v2_ethereum.ERC4626LinearPool_evt_TargetsSet
                UNION
            -- euler Linear Pools
                SELECT *, 'ethereum' AS blockchain, 'euler' AS lending_standard
                FROM balancer_v2_ethereum.EulerLinearPool_evt_TargetsSet
            ---------------------------
            --    UNION
            ---------------------------
            -- THIS CONTRACT TYPE HAS YET TO BE DECODED BY DUNE
            -- gearbox Linear Pools
                -- SELECT *, 'ethereum' AS blockchain, 'gearbox' AS lending_standard
                -- FROM balancer_v2_ethereum.GearboxLinearPool_evt_TargetsSet
            ---------------------------
                UNION
            -- yearn Linear Pools
                SELECT *, 'ethereum' AS blockchain, 'yearn' AS lending_standard
                FROM balancer_v2_ethereum.YearnLinearPool_evt_TargetsSet
                UNION
            --------------------------------------------------------------------------------------------------------------------------
            -- GNOSIS LINEAR POOL RESERVE TARGETS ------------------------------------------------------------------------------------
            --------------------------------------------------------------------------------------------------------------------------
            -- aave Linear Pools    
                SELECT 
                contract_address
                ,evt_tx_hash
                ,evt_index	
                ,evt_block_time
                ,evt_block_number
                ,lowerTarget	
                ,token
                ,upperTarget
                , 'gnosis' AS blockchain, 'aave' AS lending_standard
                FROM balancer_v2_gnosis.AaveLinearPool_evt_TargetsSet  
                UNION
            --------------------------------------------------------------------------------------------------------------------------
            -- POLYGON LINEAR POOL RESERVE TARGETS -----------------------------------------------------------------------------------
            --------------------------------------------------------------------------------------------------------------------------
            -- aave Linear Pools    
                SELECT *, 'polygon' AS blockchain, 'aave' AS lending_standard
                FROM balancer_v2_polygon.AaveLinearPool_evt_TargetsSet
                UNION
            -- erc4626 Linear Pools
                SELECT *, 'polygon' AS blockchain, 'erc4626' AS lending_standard
                FROM balancer_v2_polygon.ERC4626LinearPool_evt_TargetsSet
                UNION
            -- yearn Linear Pools
                SELECT *, 'polygon' AS blockchain, 'yearn' AS lending_standard
                FROM balancer_v2_polygon.YearnLinearPool_evt_TargetsSet
        )

        SELECT * FROM (
            SELECT *, 
                ROW_NUMBER() OVER(
                    PARTITION BY blockchain, contract_address 
                    ORDER BY blockchain ASC, evt_block_time DESC, token, evt_index DESC
                    ) as latest_update 
            FROM all_targets
        )
        WHERE latest_update = 1
    ),
    --------------------------------------------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
    -- Get all Composable Stable Pool (CSP) SwapFeePercentageChanged Events
    -- This returns all CSPs
    all_csp_fees AS (
    --------------------------------------------------------------------------------------------------------------------------
    -- ARBITRUM COMPOSABLE STABLE POOLS --------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
        SELECT *, 'arbitrum' AS blockchain
        FROM balancer_v2_arbitrum.ComposableStablePool_evt_SwapFeePercentageChanged
        UNION
    --------------------------------------------------------------------------------------------------------------------------
    -- ETHEREUM COMPOSABLE STABLE POOLS --------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
        SELECT *, 'ethereum' AS blockchain
        FROM balancer_v2_ethereum.ComposableStablePool_evt_SwapFeePercentageChanged
        UNION
    --------------------------------------------------------------------------------------------------------------------------
    -- GNOSIS COMPOSABLE STABLE POOLS ----------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
        -- SELECT *, 'gnosis' AS blockchain
        -- FROM balancer_v2_gnosis.ComposableStablePool_evt_SwapFeePercentageChanged
        -- UNION
    --------------------------------------------------------------------------------------------------------------------------
    -- POLYGON COMPOSABLE STABLE POOLS ---------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
        SELECT *, 'polygon' AS blockchain
        FROM balancer_v2_polygon.ComposableStablePool_evt_SwapFeePercentageChanged

    ),

    tokens_registered AS (
    --------------------------------------------------------------------------------------------------------------------------
    -- ARBITRUM EVERY POOL AND ITS TOKENS ------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
        SELECT *, 'arbitrum' AS blockchain
        FROM balancer_v2_arbitrum.Vault_evt_TokensRegistered 
        UNION
    --------------------------------------------------------------------------------------------------------------------------
    -- ETHEREUM EVERY POOL AND ITS TOKENS ------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
        SELECT *, 'ethereum' AS blockchain
        FROM balancer_v2_ethereum.Vault_evt_TokensRegistered 
        UNION
    --------------------------------------------------------------------------------------------------------------------------
    -- GNOSIS EVERY POOL AND ITS TOKENS --------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
        -- SELECT *, 'gnosis' AS blockchain
        -- FROM balancer_v2_gnosis.Vault_evt_TokensRegistered 
        -- UNION
    --------------------------------------------------------------------------------------------------------------------------
    -- POLYGON EVERY POOL AND ITS TOKENS -------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------------
        SELECT *, 'polygon' AS blockchain
        FROM balancer_v2_polygon.Vault_evt_TokensRegistered 

    ),
    
    latest_csp_fees AS (
        SELECT blockchain, contract_address, swapFeePercentage / POWER(10,18) AS swap_fee
        FROM (
            SELECT *, 
                ROW_NUMBER() OVER(PARTITION BY blockchain, contract_address ORDER BY blockchain ASC, evt_block_number DESC) AS latest_update 
            FROM all_csp_fees
        )
        WHERE latest_update = 1
    ),    
    
    
    unzipped_tokens_registered AS (
        WITH
            get_info AS (
                SELECT
                    poolId,
                    tokens,
                    blockchain
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER(PARTITION BY blockchain, poolId, evt_tx_hash ORDER BY blockchain ASC, evt_block_number DESC) AS latest_update
                    FROM tokens_registered 
                )
                WHERE latest_update = 1
            )
            SELECT gi.poolId, gi.blockchain, pt.tokens  --, pt.n
            FROM get_info gi
            CROSS JOIN UNNEST(gi.tokens) WITH ORDINALITY AS pt (tokens, n)
    ),
    
    csp_fees_and_tokens AS (
        SELECT csp.blockchain, csp.contract_address, csp.swap_fee, tr.poolId, tr.tokens, concat(CAST(tr.poolId AS VARCHAR),' -> ', FORMAT('%f%%', csp.swap_fee * 100)) AS pool_id_and_swap_fee
        FROM latest_csp_fees csp 
        INNER JOIN unzipped_tokens_registered tr ON csp.blockchain = tr.blockchain AND CAST(csp.contract_address AS VARCHAR) = SUBSTRING(CAST(tr.poolId AS VARCHAR),1,42)
        WHERE csp.contract_address <> tr.tokens
        ORDER BY swap_fee ASC
    ),
    
    ordered_linear_pools_in_csps AS (
        SELECT s.blockchain, s.contract_address, 
            --array_agg(csp.poolId) OVER(PARTITION BY s.blockchain, s.contract_address) AS composable_stable_pool_ids,
            array_agg(csp.pool_id_and_swap_fee) OVER(PARTITION BY s.blockchain, s.contract_address) AS composable_stable_pool_id_and_swap_fees
        FROM current_swapFee s
        INNER JOIN csp_fees_and_tokens csp ON s.blockchain = csp.blockchain AND s.contract_address = csp.tokens
    ),

    current_params AS (
        SELECT
            distinct
            s.blockchain,
            s.lending_standard,
            s.contract_address AS linear_pool, 
            swapFeePercentage/1e18 AS linear_swap_fee, 
            token, 
            lowerTarget/1e18 AS lowerTarget, 
            upperTarget/1e18 AS upperTarget,
            --csp.composable_stable_pool_ids,
            csp.composable_stable_pool_id_and_swap_fees
        FROM current_swapFee s 
        LEFT JOIN current_targets t ON (s.contract_address = t.contract_address AND s.blockchain = t.blockchain)
        LEFT JOIN ordered_linear_pools_in_csps csp ON s.blockchain = csp.blockchain AND s.contract_address = csp.contract_address
    )

SELECT * FROM current_params p

