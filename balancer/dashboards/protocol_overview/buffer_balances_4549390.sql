-- part of a query repo
-- query name: Buffer Balances
-- query link: https://dune.com/queries/4549390


WITH 
    buffer_operations AS (
        SELECT
            evt_block_time, 
            blockchain,
            event,
            wrappedToken,
            wrapped_balance,
            underlying_balance,
            value
        FROM (    
            
            SELECT
                evt_block_time,  
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
        a.blockchain,
        event,
        evt_block_time,
        m.erc4626_token_symbol,
        underlying_token_symbol,
        wrapped_balance/ POWER(10, decimals) AS erc4626_balance,
        underlying_balance/ POWER(10, decimals) AS underlying_balance,
        value / POWER(10, decimals) AS value,
        wrappedToken AS erc4626_token,
        m.underlying_token,
        LEAD(evt_block_time, 1, NOW()) OVER (PARTITION BY wrappedToken, a.blockchain ORDER BY evt_block_time) AS time_of_next_change,
        ROW_NUMBER() OVER (PARTITION BY wrappedToken ORDER BY evt_block_time DESC) AS rn
    FROM buffer_operations a
    INNER JOIN balancer_v3.erc4626_token_mapping m
    ON a.wrappedToken = m.erc4626_token
    AND a.blockchain = m.blockchain
