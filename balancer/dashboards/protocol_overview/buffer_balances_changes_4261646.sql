-- part of a query repo
-- query name: buffer_balances_changes
-- query link: https://dune.com/queries/4261646


WITH 
    buffer_operations AS (
        SELECT
            block_date, 
            wrappedToken,
                evt_tx_hash,
            SUM(wrapped_balance) AS wrapped_balance,
            SUM(underlying_balance) AS underlying_balance,
            SUM(buffer_shares) AS buffer_shares
        FROM (    
            SELECT 
                evt_block_time AS block_date, 
                wrappedToken,
                evt_tx_hash,
                SUM(CAST(amountWrapped AS INT256)) OVER (PARTITION BY wrappedToken ORDER BY evt_block_time) AS wrapped_balance,
                SUM(CAST(amountUnderlying AS INT256)) OVER (PARTITION BY wrappedToken ORDER BY evt_block_time) AS underlying_balance,
                0 AS buffer_shares
            FROM balancer_testnet_sepolia.Vault_evt_LiquidityAddedToBuffer
            WHERE contract_address = 0x30AF3689547354f82C70256894B07C9D0f067BB6
            
            UNION ALL
            
            SELECT 
                evt_block_time AS block_date, 
                wrappedToken,
                evt_tx_hash,
                0 AS wrapped_balance,
                0 AS underlying_balance,
                SUM(CAST(issuedShares AS INT256)) OVER (PARTITION BY wrappedToken ORDER BY evt_block_time) AS buffer_shares
            FROM balancer_testnet_sepolia.Vault_evt_BufferSharesMinted
            WHERE contract_address = 0x30AF3689547354f82C70256894B07C9D0f067BB6
            
            UNION ALL
            
            SELECT
                evt_block_time AS block_date, 
                wrappedToken,
                evt_tx_hash,
                SUM(CAST(mintedShares AS INT256)) OVER (PARTITION BY wrappedToken ORDER BY evt_block_time) AS wrapped_balance,
                SUM(CAST(depositedUnderlying AS INT256)) OVER (PARTITION BY wrappedToken ORDER BY evt_block_time) AS underlying_balance,
                0 AS buffer_shares
            FROM balancer_testnet_sepolia.Vault_evt_Wrap
            WHERE contract_address = 0x30AF3689547354f82C70256894B07C9D0f067BB6
            
            UNION ALL
            
            SELECT 
                evt_block_time AS block_date, 
                wrappedToken,
                evt_tx_hash,
                - SUM(CAST(amountWrapped AS INT256)) OVER (PARTITION BY wrappedToken ORDER BY evt_block_time) AS wrapped_balance,
                - SUM(CAST(amountUnderlying AS INT256)) OVER (PARTITION BY wrappedToken ORDER BY evt_block_time) AS underlying_balance,
                0 AS buffer_shares
            FROM balancer_testnet_sepolia.Vault_evt_LiquidityRemovedFromBuffer
            WHERE contract_address = 0x30AF3689547354f82C70256894B07C9D0f067BB6
            
            UNION ALL
            
            SELECT 
                evt_block_time AS block_date, 
                wrappedToken,
                evt_tx_hash,
                0 AS wrapped_balance,
                0 AS underlying_balance,
                - SUM(CAST(burnedShares AS INT256)) OVER (PARTITION BY wrappedToken ORDER BY evt_block_time) AS buffer_shares
            FROM balancer_testnet_sepolia.Vault_evt_BufferSharesBurned
            WHERE contract_address = 0x30AF3689547354f82C70256894B07C9D0f067BB6
            
            UNION ALL
            
            SELECT
                evt_block_time AS block_date, 
                wrappedToken,
                evt_tx_hash,
                - SUM(CAST(burnedShares AS INT256)) OVER (PARTITION BY wrappedToken ORDER BY evt_block_time) AS wrapped_balance,
                - SUM(CAST(withdrawnUnderlying AS INT256)) OVER (PARTITION BY wrappedToken ORDER BY evt_block_time) AS underlying_balance,
                0 AS buffer_shares
            FROM balancer_testnet_sepolia.Vault_evt_Unwrap
            WHERE contract_address = 0x30AF3689547354f82C70256894B07C9D0f067BB6
        )       
        GROUP BY 1, 2, 3
    ),
    
    adds_and_removes AS (
        SELECT 
            block_date,
            wrappedToken,
            evt_tx_hash,
            LEAD(block_date, 1, NOW()) OVER (PARTITION BY wrappedToken ORDER BY block_date) AS day_of_next_change,
            SUM(wrapped_balance) OVER (PARTITION BY wrappedToken ORDER BY block_date) AS wrapped_balance,
            SUM(underlying_balance) OVER (PARTITION BY wrappedToken ORDER BY block_date) AS underlying_balance,
            SUM(buffer_shares) OVER (PARTITION BY wrappedToken ORDER BY block_date) AS shares_balance
        FROM buffer_operations
    ),
    
    adds_and_removes_with_lag AS (
        SELECT 
            *,
            LAG(wrapped_balance) OVER (PARTITION BY wrappedToken ORDER BY block_date) AS previous_balance -- previous balance calculation
        FROM adds_and_removes
    ),

    final AS(
        SELECT
            a.block_date,
            a.wrappedToken,
            evt_tx_hash,
            COALESCE(wrapped_balance, 0) AS wrapped_balance,
            COALESCE(previous_balance, 0) AS previous_balance,
            COALESCE(wrapped_balance, 0) - COALESCE(previous_balance, 0) AS wrapped_balance_change, 
            COALESCE(underlying_balance, 0) AS underlying_balance
        FROM adds_and_removes_with_lag a
        WHERE a.wrappedToken IS NOT NULL
        ORDER BY 1 DESC, 2
    )
    
SELECT * FROM final;
