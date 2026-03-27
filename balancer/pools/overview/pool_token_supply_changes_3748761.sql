-- part of a query repo
-- query name: Pool token supply changes
-- query link: https://dune.com/queries/3748761


WITH pool_labels AS (
        SELECT
            address,
            name,
            pool_type
        FROM labels.balancer_v2_pools
        WHERE blockchain = 'arbitrum'
    ),

      -- Extract mints and burns from transfers
    transfers AS (
        SELECT
            t.evt_block_time,
            t.evt_block_number,
            t.evt_tx_hash,
            t.evt_index,
            t.contract_address AS token,
            CASE 
                WHEN t."from" = 0x0000000000000000000000000000000000000000 
                THEN 'mint' 
                WHEN t.to = 0x0000000000000000000000000000000000000000 
                THEN 'burn'
                END AS label,
            l.pool_type,
            l.name,
            CASE 
                WHEN t."from" = 0x0000000000000000000000000000000000000000 
                THEN value 
                WHEN t.to = 0x0000000000000000000000000000000000000000 
                THEN - value
                ELSE 0
                END AS amount
        FROM balancer.transfers_bpt t
        LEFT JOIN pool_labels l ON t.contract_address = l.address
        WHERE t.blockchain = 'arbitrum'
        AND t.version = '2'
    ),

    -- Calculating Joins(mint) and Exits(burn) via Swap
    joins AS (
        SELECT 
            s.evt_block_time,
            s.evt_block_number,
            s.evt_tx_hash, 
            s.evt_index,
            s.tokenOut AS token,
            'join' AS label,
            l.pool_type,
            l.name,
            CASE WHEN l.pool_type IN ('weighted') 
            THEN 0
            ELSE s.amountOut 
            END AS amount
        FROM balancer_v2_arbitrum.Vault_evt_Swap s
        LEFT JOIN pool_labels l ON BYTEARRAY_SUBSTRING(s.poolId, 1, 20) = l.address
        WHERE tokenOut = BYTEARRAY_SUBSTRING(s.poolId, 1, 20)

    ),

    exits AS (
        SELECT 
            s.evt_block_time,
            s.evt_block_number,
            s.evt_tx_hash, 
            s.evt_index,
            s.tokenIn AS token,
            'exit' AS label,
            l.pool_type,
            l.name,
            CASE WHEN l.pool_type IN ('weighted') 
            THEN 0
            ELSE - s.amountIn
            END AS amount
        FROM balancer_v2_arbitrum.Vault_evt_Swap s
        LEFT JOIN pool_labels l ON BYTEARRAY_SUBSTRING(s.poolId, 1, 20) = l.address
        WHERE tokenIn = BYTEARRAY_SUBSTRING(s.poolId, 1, 20)
    ),
    
    spell AS(     SELECT
            date_trunc('day', evt_block_time) AS block_date,
            evt_block_time,
            evt_block_number,
            'arbitrum' AS blockchain,
            evt_tx_hash,
            evt_index,
            pool_type,
            name AS pool_symbol,
            '2' AS version,
            label,
            token AS token_address,
            amount AS delta_amount_raw,
            amount / POWER (10, 18) AS delta_amount --18 decimals standard for BPTs
    FROM 
    (
        SELECT 
            *
        FROM joins 
        
        UNION ALL
        
        SELECT 
            *
        FROM exits
        
        UNION ALL
        
        SELECT 
            *
        FROM transfers        
        WHERE label IS NOT NULL
            )
        WHERE  date_trunc('day', evt_block_time) = CURRENT_DATE)
        SELECT * FROM spell
