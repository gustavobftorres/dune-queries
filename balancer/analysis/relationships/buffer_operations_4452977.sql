-- part of a query repo
-- query name: Buffer Operations
-- query link: https://dune.com/queries/4452977


WITH 
    buffer_operations AS (
        SELECT
            evt_block_time,   
            evt_tx_hash,
            blockchain,
            event,
            wrappedToken,
            wrapped_balance,
            underlying_balance,
            value
        FROM (    
            
            SELECT
                evt_block_time,  
                evt_tx_hash,
                'ethereum' AS blockchain,
                'wrap' AS event,
                wrappedToken,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 1, 16)) AS wrapped_balance,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 17, 32)) AS underlying_balance,
                depositedUnderlying AS value
            FROM balancer_v3_ethereum.Vault_evt_Wrap
            
            UNION ALL
            
            SELECT
                evt_block_time,  
                evt_tx_hash,  
                'ethereum' AS blockchain,
                'unwrap' AS event,
                wrappedToken,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 1, 16)) AS wrapped_balance,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 17, 32)) AS underlying_balance,
                withdrawnUnderlying AS value
            FROM balancer_v3_ethereum.Vault_evt_Unwrap
                  
            UNION ALL
            
            SELECT
                evt_block_time,    
                evt_tx_hash,
                'gnosis' AS blockchain,
                'wrap' AS event,
                wrappedToken,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 1, 16)) AS wrapped_balance,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 17, 32)) AS underlying_balance,
                depositedUnderlying AS value
            FROM balancer_v3_gnosis.Vault_evt_Wrap
            
            UNION ALL
            
            SELECT
                evt_block_time,    
                evt_tx_hash,
                'gnosis' AS blockchain,
                'unwrap' AS event,
                wrappedToken,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 1, 16)) AS wrapped_balance,
                varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances, 17, 32)) AS underlying_balance,
                withdrawnUnderlying AS value
            FROM balancer_v3_gnosis.Vault_evt_Unwrap
        )       
    )

    SELECT
        evt_block_time,  
        evt_tx_hash,
        event,
        wrapped_balance/ POWER(10, decimals) AS wrapped_balance,
        wrapped_balance/ POWER(10, decimals) AS underlying_balance,
        value / POWER(10, decimals) AS value
    FROM buffer_operations a
    INNER JOIN balancer_v3.erc4626_token_mapping m
    ON a.wrappedToken = m.erc4626_token
    WHERE wrappedToken = {{wrapped_token}}
    AND a.blockchain = '{{blockchain}}'
    ORDER BY 1 DESC