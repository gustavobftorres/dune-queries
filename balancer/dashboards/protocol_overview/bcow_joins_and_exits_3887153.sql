-- part of a query repo
-- query name: bcow_joins_and_exits
-- query link: https://dune.com/queries/3887153


WITH joins AS (
        SELECT 
            DATE_TRUNC('day', evt_block_time) AS block_date, 
            evt_block_number,
            evt_tx_hash,
            evt_index,
            contract_address AS pool,
            'remove' AS type, 
            caller AS user,
            tokenIn,
            tokenAmountIn / POWER(10, 18) AS ajoins
        FROM balancer_testnet_sepolia.BCOWPool_evt_LOG_JOIN 
        WHERE tokenIn = contract_address    
    ),

    exits AS (
        SELECT 
            DATE_TRUNC('day', evt_block_time) AS block_date, 
            evt_block_number,
            evt_tx_hash,
            evt_index,
            contract_address AS pool,
            'Add' AS type, 
            caller AS user,
            tokenOut,
            - tokenAmountOut / POWER(10, 18) AS aexits
        FROM balancer_testnet_sepolia.BCOWPool_evt_LOG_EXIT       
        WHERE tokenOut = contract_address
    )
        SELECT 
            * 
        FROM exits
        
        UNION 
        
        SELECT 
            *
        FROM joins
