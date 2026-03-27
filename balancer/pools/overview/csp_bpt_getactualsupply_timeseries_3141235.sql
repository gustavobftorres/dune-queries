-- part of a query repo
-- query name: CSP BPT getActualSupply Timeseries
-- query link: https://dune.com/queries/3141235


WITH
    sub_pools AS (
        SELECT
            contract_address
            , evt_tx_hash
            , evt_index
            , evt_block_time
            , evt_block_number
            , assetManagers
            , poolId
            , bytearray_substring(x.poolId, 1, 20) AS pool_token
            -- Create a new array where the Pool Token is at position 1
            , ARRAY[bytearray_substring(x.poolId, 1, 20)] || array_remove(tokens, bytearray_substring(x.poolId, 1, 20)) AS tokens
        FROM balancer_v2_{{blockchain}}.Vault_evt_TokensRegistered x
        WHERE {{pool_id}} = x.poolId
    )
    
    , join_exit_call_data AS ( 
        SELECT 
            evt_block_time
            , evt_block_number
            , evt_tx_hash
            , l.tx_index
            , evt_index
            , poolId
            , lp_token_burned
            , lp_token_minted
            , delta_pool_token
            , pool_token
            , deltas
            , ARRAY[
                CASE 
                    WHEN delta_pool_token > INT256 '0' THEN
                    CASE 
                        WHEN lp_token_burned > INT256 '0' THEN 0x00 
                        ELSE pool_token 
                    END
                    ELSE pool_token
                END
            ] AS tokens_in

            , ARRAY[
                CASE
                    WHEN delta_pool_token <= INT256 '0' THEN
                    CASE 
                        WHEN lp_token_burned > INT256 '0' THEN 0x00 
                        ELSE pool_token 
                    END
                    ELSE pool_token
                END
            ] AS tokens_out
    
            , ARRAY[
                CASE 
                    WHEN delta_pool_token > INT256 '0' THEN lp_token_minted
                    ELSE lp_token_minted
                END
            ] AS amounts_in
    
            , ARRAY[
                CASE 
                WHEN delta_pool_token > INT256 '0' THEN delta_pool_token + lp_token_burned
                    ELSE lp_token_burned
                END
            ] AS amounts_out
        FROM ( --Start subquery
            SELECT 
                x.contract_address
                , x.evt_tx_hash
                , x.evt_index
                , x.evt_block_time
                , x.evt_block_number
                , x.poolId AS poolId
                , sp.pool_token AS pool_token
                , x.deltas
                , x.deltas[ARRAY_POSITION(x.tokens, sp.pool_token)] AS delta_pool_token
                , y.lp_token_burned
                , y.lp_token_minted
            FROM balancer_v2_{{blockchain}}.Vault_evt_PoolBalanceChanged x
            INNER JOIN (SELECT poolId, tokens, pool_token FROM sub_pools) sp
                ON sp.poolId = x.poolId
            INNER JOIN (
                SELECT 
                    evt_tx_hash
                    , contract_address
                    , IF(
                        sum(CAST(value AS int256)) FILTER(WHERE "from" = 0x0000000000000000000000000000000000000000) IS NULL
                        , int256 '0'
                        , sum(CAST(value AS int256)) FILTER(WHERE "from" = 0x0000000000000000000000000000000000000000)
                    ) AS lp_token_minted
                    , IF(
                        sum(CAST(value AS int256)) FILTER(WHERE to = 0x0000000000000000000000000000000000000000) IS NULL
                        , int256 '0'
                        , sum(CAST(value AS int256)) FILTER(WHERE to = 0x0000000000000000000000000000000000000000)
                    ) AS lp_token_burned
                FROM evms.erc20_transfers 
                WHERE blockchain = '{{blockchain}}'
                    AND evt_block_number >= (SELECT min(evt_block_number) FROM sub_pools) --####################################
                    AND (to = 0x0000000000000000000000000000000000000000 OR "from" = 0x0000000000000000000000000000000000000000)
                    AND contract_address = (SELECT pool_token FROM sub_pools)
                GROUP BY 1, 2
            ) y
                ON y.evt_tx_hash = x.evt_tx_hash
        ) x -- End subquery
        -- Join transactions to get additional data
        INNER JOIN (
            SELECT
                blockchain
                , block_number
                , hash AS tx_hash
                , index AS tx_index
                , "from" AS tx_from
                , "to" AS tx_to
            FROM evms.transactions
            WHERE blockchain = '{{blockchain}}'
            AND block_number >= (SELECT min(evt_block_number) FROM sub_pools)
        ) l
        ON l.block_number = x.evt_block_number
        AND l.tx_hash     = x.evt_tx_hash
    ) -- End CTE
    
    , swap_data AS (
            SELECT
                evt_block_time
                , evt_block_number
                , swaps.evt_tx_hash
                , txns.tx_index
                , evt_index
                , swaps.poolId
                , CASE WHEN tokenIn = tokens[1] THEN array[tokenIn] ELSE NULL--ARRAY[0x00]
                  END AS tokens_in
                , CASE WHEN tokenOut = tokens[1] THEN array[tokenOut] ELSE NULL--ARRAY[0x00]
                  END AS tokens_out
                , CASE WHEN tokenIn = tokens[1] THEN array[IF(lp_token_minted IS NULL, INT256 '0', lp_token_minted) - amountIn] ELSE ARRAY[INT256 '0'] END AS amounts_in
                , CASE WHEN tokenOut = tokens[1] THEN array[IF(lp_token_burned IS NULL, INT256 '0', lp_token_burned) - amountOut] ELSE ARRAY[INT256 '0'] END AS amounts_out
                , txns.tx_from
                , txns.tx_to
                , tokens
                , tokenIn
                , tokenOut
                -- Table that computes these values does not calculate them if there is no transfer to/from 0x00
                -- Have to add in the 0 to make it work properly
                , IF(lp_token_minted IS NULL, INT256 '0', lp_token_minted) AS lp_token_minted
                , IF(lp_token_burned IS NULL, INT256 '0', lp_token_burned) AS lp_token_burned
                , amountIn
                , amountOut
                --, tokenIn = tokens[1]
            FROM (
                -- Get the swap evts from the Vault
                SELECT 
                    contract_address
                    , evt_tx_hash
                    , evt_index
                    , evt_block_time
                    , evt_block_number
                    , CAST(amountIn AS int256) AS amountIn -- Change to match type of join_exit_call_data CTE
                    , CAST(amountOut AS int256) AS amountOut -- ^
                    , x.poolId
                    , tokenIn
                    , tokenOut
                    , tokens
                FROM balancer_v2_{{blockchain}}.Vault_evt_Swap x
                -- Token list for each pool_id in the ComposableStablePool
                INNER JOIN (SELECT poolId, tokens FROM sub_pools) sp
                    ON sp.poolId = x.poolId
            ) swaps
            -- Get the EOA that started the txn as tx_from
            INNER JOIN (
                SELECT
                    block_number
                    , hash AS tx_hash
                    , index AS tx_index
                    , "from" AS tx_from
                    , "to" AS tx_to
                FROM evms.transactions
                WHERE  blockchain = '{{blockchain}}'
                AND block_number >= (SELECT min(evt_block_number) FROM sub_pools)
            ) txns
                ON txns.block_number = swaps.evt_block_number
                AND txns.tx_hash     = swaps.evt_tx_hash
            LEFT JOIN (
                SELECT 
                    evt_tx_hash
                    , contract_address
                    , IF(
                        sum(CAST(value AS int256)) FILTER(WHERE "from" = 0x0000000000000000000000000000000000000000) IS NULL
                        , int256 '0'
                        , sum(CAST(value AS int256)) FILTER(WHERE "from" = 0x0000000000000000000000000000000000000000)
                    ) AS lp_token_minted
                    , IF(
                        sum(CAST(value AS int256)) FILTER(WHERE to = 0x0000000000000000000000000000000000000000) IS NULL
                        , int256 '0'
                        , sum(CAST(value AS int256)) FILTER(WHERE to = 0x0000000000000000000000000000000000000000)
                    ) AS lp_token_burned
                FROM evms.erc20_transfers 
                WHERE blockchain = '{{blockchain}}'
                    AND evt_block_number >= (SELECT min(evt_block_number) FROM sub_pools) --####################################
                    AND (to = 0x0000000000000000000000000000000000000000 OR "from" = 0x0000000000000000000000000000000000000000)
                    AND contract_address = (SELECT pool_token FROM sub_pools)
                GROUP BY 1, 2
            ) mint_burn
                ON mint_burn.evt_tx_hash = swaps.evt_tx_hash
    )
    
    
    --select * from swap_data
    
    
    
    , pool_balance_managed AS (
        SELECT distinct
            action_type
            , evt_block_time
            , evt_block_number
            , evt_index
            , evt_tx_hash
            , poolId
            , token
            , CASE WHEN token = tokens[1] AND balance_change > int256 '0' THEN array[token, 0x00, 0x00]
                  ELSE array[0x00, 0x00, 0x00]
            END AS tokens_in
            , CASE WHEN token = tokens[1] AND balance_change < int256 '0' THEN array[token, 0x00, 0x00]
                  ELSE array[0x00, 0x00, 0x00]
            END AS tokens_out
            , CASE WHEN token = tokens[1] AND balance_change > int256 '0' THEN array[abs(balance_change), int256 '0', int256 '0']
                  ELSE array[int256 '0', int256 '0', int256 '0']
            END AS amounts_in
            , CASE WHEN token = tokens[1] AND balance_change < int256 '0' THEN array[abs(balance_change), int256 '0', int256 '0']
                  ELSE array[int256 '0', int256 '0', int256 '0']
            END AS amounts_out
            , transfer_value -- from erc20 transfer events
            , balance_change -- from PoolBalanceManaged events
            , balance_change = transfer_value AS balance_change_is_value_transfer -- Checks that the PoolBalanceManaged resulting changes match the actual settlement via transfers
            , tokens
        FROM (
            SELECT
                'PoolBalanceManaged' AS action_type
                , y.contract_address
                , x.evt_tx_hash
                , y.evt_index
                , x.evt_block_time
                , x.evt_block_number
                , x.assetManager
                , x.cashDelta
                , x.managedDelta
                , x.pool_id AS poolId
                , x.token
                , x.tokens
                , y.to
                , y."from"
                , y.transfer_value
                , managedDelta AS balance_change--OVER(PARTITION BY x.evt_tx_hash, x.pool_id, x.token) AS balance_change 
            FROM (
                SELECT *
                    , z.poolId AS pool_id 
                FROM balancer_v2_{{blockchain}}.Vault_evt_PoolBalanceManaged z
                INNER JOIN (SELECT poolId, tokens FROM sub_pools) sp
                    ON sp.poolId = z.poolId
                WHERE cashDelta = int256 '0'
            ) x
            INNER JOIN (
                SELECT 
                    blockchain, "to", "from", contract_address, evt_index, evt_tx_hash, CAST(value AS int256) AS transfer_value 
                FROM evms.erc20_transfers
                WHERE blockchain = '{{blockchain}}'
                AND evt_block_number >= (SELECT min(evt_block_number) FROM sub_pools) --###################################
                AND "to" = 0xba12222222228d8ba445958a75a0704d566bf2c8
                UNION ALL
                SELECT 
                    blockchain, "to", "from", contract_address, evt_index, evt_tx_hash, -CAST(value AS int256) AS transfer_value 
                FROM evms.erc20_transfers
                WHERE blockchain = '{{blockchain}}'
                AND evt_block_number >= (SELECT min(evt_block_number) FROM sub_pools) --###################################
                AND "from" = 0xba12222222228d8ba445958a75a0704d566bf2c8
            ) y
            ON y.evt_tx_hash = x.evt_tx_hash
            AND y.contract_address = x.token
        ) WHERE (balance_change = transfer_value) = True -- Have to add in where the transfer evt is max but less than pool balance managed part
    )
    
    , join_exit_swap AS (
        SELECT
            evt_block_time AS block_time
            , evt_block_number AS block_number
            , evt_tx_hash AS tx_hash
            , tx_index AS tx_index
            , evt_index AS action_index
            , poolId AS pool_id
            , tokens_in
            , tokens_out
            , amounts_in
            , amounts_out
            --, tokens
        FROM join_exit_call_data
        UNION ALL 
        SELECT
            evt_block_time AS block_time
            , evt_block_number AS block_number
            , evt_tx_hash AS tx_hash
            , tx_index AS tx_index
            , evt_index AS action_index
            , poolId AS pool_id
            , tokens_in
            , tokens_out
            , amounts_in
            , amounts_out
            --, tokens
        FROM swap_data
        UNION ALL 
        SELECT
            evt_block_time AS block_time
            , evt_block_number AS block_number
            , evt_tx_hash AS tx_hash
            , NULL AS tx_index
            , evt_index AS action_index
            , poolId AS pool_id
            , tokens_in
            , tokens_out
            , amounts_in
            , amounts_out
            --, tokens
        FROM pool_balance_managed
    )
    
    , t_erc20 AS (
    -- Get token decimals and symbol
    -- Since it is joined multiple times for each token in the linear pool
    -- it is more efficient to have join from a CTE
        SELECT 
            contract_address AS c
            , blockchain AS b
            , symbol AS s
            , decimals AS d 
        FROM tokens.erc20
    )
    
    , data_table_1 AS (
        SELECT 
            block_time
            , block_number
            , tx_hash
            , tx_index
            , action_index
            , pool_id
            , tokens_in
            , tokens_out
            , amounts_in
            , amounts_out
            , zip_with(
                amounts_in -- Array 1 length = 3
                , amounts_out -- Array 2 length = 3
                , (x, y) -> x - y -- Function x = entry in array 1, y = entry in array 2, perform operation x - y
            ) AS array_in_minus_out
            , ARRAY[
                amounts_in[1] / POWER(10, COALESCE(lp.d, 18))
            ] AS amounts_in_scaled
            , ARRAY[
                amounts_out[1] / POWER(10, COALESCE(lp.d, 18))
            ] AS amounts_out_scaled
            , lp.s -- token_1 (LP) symbol
            , COALESCE(lp.d, 18) AS lp_d -- tokne_1 (LP) decimals (18)
        FROM join_exit_swap x
        LEFT JOIN t_erc20 lp
            ON  lp.b = '{{blockchain}}' 
            AND lp.c = bytearray_substring(pool_id, 1, 20)
    )
    
    , data_table_2 AS (
        SELECT 
            block_time
            , block_number
            , tx_hash
            , action_index
            , pool_id
            , tokens_in
            , tokens_out
            , amounts_in
            , amounts_out
            , array_in_minus_out
            , amounts_in_scaled
            , amounts_out_scaled
            , sum(array_in_minus_out[1]) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC ROWS UNBOUNDED PRECEDING) AS lp_virtual_supply_raw
            , sum(array_in_minus_out[1] / POWER(10, lp_d)) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC ROWS UNBOUNDED PRECEDING) 
                AS lp_virtual_supply
        FROM data_table_1
    )

    , data_table_3 AS (
        SELECT *
            -- Get the previous supply amount for each token
            , LAG(lp_virtual_supply) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC) AS prev_lp_supply
        FROM data_table_2
    )


SELECT 
    block_time
    , block_number
    , lp_virtual_supply_raw
    , lp_virtual_supply
    , pool_id
    , action_index
    , tx_hash
 --   , tokens_in
 --   , tokens_out
    , amounts_in
    , amounts_out
    , array_in_minus_out
    , amounts_in_scaled
    , amounts_out_scaled
FROM data_table_3
ORDER BY block_number ASC, action_index ASC, pool_id

