-- part of a query repo
-- query name: Buffer Transactions
-- query link: https://dune.com/queries/4144874


WITH 
    buffer_operations AS (
        SELECT
            evt_block_time, 
            evt_tx_hash,
            blockchain,
            wrappedToken,
            wrapped_balance,
            underlying_balance,
            buffer_shares
        FROM (    
            SELECT 
                evt_block_time, 
                evt_tx_hash,
                'ethereum' AS blockchain,
                wrappedToken,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 1, 16)) AS wrapped_balance,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 17, 32)) AS underlying_balance,
                0 AS buffer_shares
            FROM balancer_v3_ethereum.Vault_evt_LiquidityAddedToBuffer
            
            UNION ALL
            
            SELECT 
                evt_block_time,   
                evt_tx_hash,
                'ethereum' AS blockchain,
                wrappedToken,              
                0 AS wrapped_balance,
                0 AS underlying_balance,
                CAST(issuedShares AS INT256) AS buffer_shares
            FROM balancer_v3_ethereum.Vault_evt_BufferSharesMinted
            
            
            UNION ALL
            
            SELECT
                evt_block_time,   
                evt_tx_hash,
                'ethereum' AS blockchain,
                wrappedToken,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 1, 16)) AS wrapped_balance,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 17, 32)) AS underlying_balance,
                0 AS buffer_shares
            FROM balancer_v3_ethereum.Vault_evt_Wrap
            
            
            UNION ALL
            
            SELECT 
                evt_block_time,   
                evt_tx_hash,
                'ethereum' AS blockchain,
                wrappedToken,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 1, 16)) AS wrapped_balance,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 17, 32)) AS underlying_balance,
                0 AS buffer_shares
            FROM balancer_v3_ethereum.Vault_evt_LiquidityRemovedFromBuffer
            
            UNION ALL
            
            SELECT 
                evt_block_time,   
                evt_tx_hash,
                'ethereum' AS blockchain,
                wrappedToken,
                0 AS wrapped_balance,
                0 AS underlying_balance,
                - CAST(burnedShares AS INT256) AS buffer_shares
            FROM balancer_v3_ethereum.Vault_evt_BufferSharesBurned
            
            UNION ALL
            
            SELECT
                evt_block_time,   
                evt_tx_hash,
                'ethereum' AS blockchain,
                wrappedToken,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 1, 16)) AS wrapped_balance,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 17, 32)) AS underlying_balance,
                0 AS buffer_shares
            FROM balancer_v3_ethereum.Vault_evt_Unwrap

            UNION ALL

            SELECT 
                evt_block_time,  
                evt_tx_hash,
                'gnosis' AS blockchain,
                wrappedToken,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 1, 16)) AS wrapped_balance,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 17, 32)) AS underlying_balance,
                0 AS buffer_shares
            FROM balancer_v3_gnosis.Vault_evt_LiquidityAddedToBuffer
            
            UNION ALL
            
            SELECT 
                evt_block_time,   
                evt_tx_hash,
                'gnosis' AS blockchain,
                wrappedToken,              
                0 AS wrapped_balance,
                0 AS underlying_balance,
                CAST(issuedShares AS INT256) AS buffer_shares
            FROM balancer_v3_gnosis.Vault_evt_BufferSharesMinted
            
            
            UNION ALL
            
            SELECT
                evt_block_time,   
                evt_tx_hash,
                'gnosis' AS blockchain,
                wrappedToken,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 1, 16)) AS wrapped_balance,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 17, 32)) AS underlying_balance,
                0 AS buffer_shares
            FROM balancer_v3_gnosis.Vault_evt_Wrap
            
            
            UNION ALL
            
            SELECT 
                evt_block_time,   
                evt_tx_hash,
                'gnosis' AS blockchain,
                wrappedToken,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 1, 16)) AS wrapped_balance,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 17, 32)) AS underlying_balance,
                0 AS buffer_shares
            FROM balancer_v3_gnosis.Vault_evt_LiquidityRemovedFromBuffer
            
            UNION ALL
            
            SELECT 
                evt_block_time,   
                evt_tx_hash,
                'gnosis' AS blockchain,
                wrappedToken,
                0 AS wrapped_balance,
                0 AS underlying_balance,
                - CAST(burnedShares AS INT256) AS buffer_shares
            FROM balancer_v3_gnosis.Vault_evt_BufferSharesBurned
            
            UNION ALL
            
            SELECT
                evt_block_time, 
                evt_tx_hash,
                'gnosis' AS blockchain,
                wrappedToken,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 1, 16)) AS wrapped_balance,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 17, 32)) AS underlying_balance,
                0 AS buffer_shares
            FROM balancer_v3_gnosis.Vault_evt_Unwrap
        )       
    ),
    
    adds_and_removes AS (
        SELECT 
            evt_block_time,  
            evt_tx_hash,
            blockchain,
            wrappedToken,
            LEAD(evt_block_time, 1,  now()) OVER (PARTITION BY wrappedToken ORDER BY evt_block_time) AS time_of_next_change,
            wrapped_balance AS wrapped_balance,
            underlying_balance,
            SUM(buffer_shares) OVER (PARTITION BY wrappedToken ORDER BY evt_block_time) AS shares_balance
        FROM buffer_operations
    )

SELECT
    evt_block_time, 
    evt_tx_hash,
    a.blockchain,
    wrappedToken,
    erc4626_token_name AS symbol,
    wrapped_balance / POWER(10,decimals)  AS wrapped_balance,
    underlying_balance / POWER(10,decimals)  AS underlying_balance,
    shares_balance / POWER(10,decimals)  AS shares_balance,
    time_of_next_change,
    ROW_NUMBER() OVER (PARTITION BY wrappedToken ORDER BY evt_block_time DESC) AS rn
FROM adds_and_removes a
INNER JOIN balancer_v3.erc4626_token_mapping m
ON a.wrappedToken = m.erc4626_token
AND a.blockchain = m.blockchain           
ORDER BY 1 DESC, 6;
