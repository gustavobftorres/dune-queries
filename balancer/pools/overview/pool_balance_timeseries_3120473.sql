-- part of a query repo
-- query name: Pool Balance Timeseries
-- query link: https://dune.com/queries/3120473


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
            , CASE -- Hard code an ordered token array structure for future use
                WHEN bytearray_substring(x.poolId, 1, 20) = tokens[1] THEN array[tokens[1], tokens[2], tokens[3]]
                WHEN bytearray_substring(x.poolId, 1, 20) = tokens[2] THEN array[tokens[2], tokens[1], tokens[3]]
                WHEN bytearray_substring(x.poolId, 1, 20) = tokens[3] THEN array[tokens[3], tokens[1], tokens[2]]
            END AS tokens
        FROM balancer_v2_{{blockchain}}.Vault_evt_TokensRegistered x
        WHERE {{pool_id}} = x.poolId
    )
    
    , join_exit_call_data AS ( 
        SELECT 
            CASE WHEN delta_pool_token <> int256 '0' THEN 'join' ELSE 'exit' END AS action_type
            , evt_block_time
            , evt_block_number
            , evt_tx_hash
            , l.tx_index
            , evt_index
            , poolId
            , lp_token_burned
            , lp_token_minted
            , delta_pool_token
            , delta_token_1
            , delta_token_2
            , deltas
            , array[
                -- IF(delta_pool_token > int256 '0'
                --     , IF(lp_token_burned > int256 '0', 0x00, pool_token)
                --     , pool_token
                -- )
                IF((lp_token_minted - lp_token_burned) < int256 '0', pool_token, 0x00)
                , IF(delta_token_1 > int256 '0', token_1, 0x00)
                , IF(delta_token_2 > int256 '0', token_2, 0x00)
            ] AS tokens_in
            
            , array[
                -- IF(delta_pool_token <= int256 '0' 
                --     , IF(lp_token_burned > int256 '0', 0x00, pool_token)
                --     , pool_token
                -- )
                IF((lp_token_minted - lp_token_burned) > int256 '0', pool_token, 0x00)
                , IF(delta_token_1 < int256 '0', token_1, 0x00)
                , IF(delta_token_2 < int256 '0', token_2, 0x00)
            ] AS tokens_out
            
            , array[
                IF(delta_pool_token > int256 '0' 
                    , delta_pool_token
                    --, IF(lp_token_burned > int256 '0', lp_token_burned, int256 '0')
                    , IF((lp_token_minted - lp_token_burned) < int256 '0', abs(lp_token_minted - lp_token_burned), int256 '0')
                )
                --IF((lp_token_minted - lp_token_burned) < int256 '0', (lp_token_minted - lp_token_burned), int256 '0')
                , IF(delta_token_1 >= int256 '0', delta_token_1, int256 '0')
                , IF(delta_token_2 >= int256 '0', delta_token_2, int256 '0')
            ] AS amounts_in
            
            , array[
                IF(delta_pool_token <= int256 '0' 
                    --, IF(lp_token_burned <= int256 '0', lp_token_burned, int256 '0')
                    , IF((lp_token_minted - lp_token_burned) > int256 '0', abs(lp_token_minted - lp_token_burned), int256 '0')
                    , delta_pool_token
                )
                --IF((lp_token_minted - lp_token_burned) > int256 '0', (lp_token_minted - lp_token_burned), int256 '0')
                , IF(delta_token_1 < int256 '0', -1 * delta_token_1, int256 '0')
                , IF(delta_token_2 < int256 '0', -1 * delta_token_2, int256 '0')
            ] AS amounts_out

            --, IF(delta_pool_token = int256 '0', lp_token_burned, delta_pool_token)AS lp_token_delta
            , lp_token_minted - lp_token_burned AS lp_token_delta
            , array[pool_token, token_1, token_2] = tokens AS asset_array_equals_token_array -- Asset array from join/exit = token array from pool created
            
            , liquidityProvider
            -- Log data
            
            , l.tx_to
            , l.tx_from --AS trader
            , liquidityProvider = l.tx_from AS lp_is_trader
            , tokens
        FROM ( --Start subquery
            SELECT 
                x.contract_address
                , x.evt_tx_hash
                , x.evt_index
                , x.evt_block_time
                , x.evt_block_number
                , x.poolId AS poolId
                , sp.pool_token AS pool_token
                , sp.tokens[2] AS token_1
                , sp.tokens[3] AS token_2
                , x.deltas
                , CASE WHEN sp.tokens[1] = x.tokens[1] THEN x.deltas[1]
                       WHEN sp.tokens[1] = x.tokens[2] THEN x.deltas[2]
                       WHEN sp.tokens[1] = x.tokens[3] THEN x.deltas[3]
                END AS delta_pool_token
                , CASE WHEN sp.tokens[2] = x.tokens[1] THEN x.deltas[1]
                       WHEN sp.tokens[2] = x.tokens[2] THEN x.deltas[2]
                       WHEN sp.tokens[2] = x.tokens[3] THEN x.deltas[3] 
                END AS delta_token_1
                , CASE WHEN sp.tokens[3] = x.tokens[1] THEN x.deltas[1]
                       WHEN sp.tokens[3] = x.tokens[2] THEN x.deltas[2]
                       WHEN sp.tokens[3] = x.tokens[3] THEN x.deltas[3]
                END AS delta_token_2
                , CASE WHEN y.to = 0x0000000000000000000000000000000000000000 THEN sum(CAST(y.value AS int256)) OVER(PARTITION BY y.evt_tx_hash) ELSE int256 '0' END AS lp_token_burned
                , CASE WHEN y."from" = 0x0000000000000000000000000000000000000000 THEN sum(CAST(y.value AS int256)) OVER(PARTITION BY y.evt_tx_hash) ELSE int256 '0' END AS lp_token_minted
                , x.liquidityProvider
                , sp.tokens = x.tokens
                , sp.tokens
            FROM balancer_v2_{{blockchain}}.Vault_evt_PoolBalanceChanged x
            INNER JOIN (SELECT poolId, tokens, pool_token FROM sub_pools) sp
                ON sp.poolId = x.poolId
            INNER JOIN (
                SELECT * FROM evms.erc20_transfers 
                WHERE blockchain = '{{blockchain}}'
                AND evt_block_number >= (SELECT min(evt_block_number) FROM sub_pools) --####################################
            ) y
                ON y.evt_tx_hash = x.evt_tx_hash
                AND y.contract_address = sp.pool_token
                AND (y.to = 0x0000000000000000000000000000000000000000 OR y."from" = 0x0000000000000000000000000000000000000000)
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
    
    --SELECT * FROM join_exit_call_data
    
    
    , swap_data AS (
            SELECT
                action_type
                , evt_block_time
                , evt_block_number
                , evt_tx_hash
                , txns.tx_index
                , evt_index
                , swaps.poolId
                , CASE WHEN tokenIn = tokens[1] THEN array[tokenIn, 0x00, 0x00]
                      WHEN tokenIn = tokens[2] THEN array[0x00, tokenIn, 0x00]
                      WHEN tokenIn = tokens[3] THEN array[0x00, 0x00, tokenIn]
                END AS tokens_in
                , CASE WHEN tokenOut = tokens[1] THEN array[tokenOut, 0x00, 0x00]
                      WHEN tokenOut = tokens[2] THEN array[0x00, tokenOut, 0x00]
                      WHEN tokenOut = tokens[3] THEN array[0x00, 0x00, tokenOut]
                END AS tokens_out
                , CASE WHEN tokenIn = tokens[1] THEN array[amountIn, int256 '0', int256 '0']
                      WHEN tokenIn = tokens[2] THEN array[int256 '0', amountIn, int256 '0']
                      WHEN tokenIn = tokens[3] THEN array[int256 '0', int256 '0', amountIn]
                END AS amounts_in
                , CASE WHEN tokenOut = tokens[1] THEN array[amountOut, int256 '0', int256 '0']
                      WHEN tokenOut = tokens[2] THEN array[int256 '0', amountOut, int256 '0']
                      WHEN tokenOut = tokens[3] THEN array[int256 '0', int256 '0', amountOut]
                END AS amounts_out
                , txns.tx_from
                , txns.tx_to
                , tokens
            FROM (
                -- Get the swap evts from the Vault
                SELECT
                    -- Categorize the swap type
                    CASE WHEN tokenIn = bytearray_substring(x.poolId, 1, 20) THEN 'swap_exit'
                         WHEN tokenOut = bytearray_substring(x.poolId, 1, 20) THEN 'swap_join'
                         WHEN tokenIn <> bytearray_substring(x.poolId, 1, 20) AND tokenOut <> bytearray_substring(x.poolId, 1, 20) THEN 'swap'
                    END AS action_type
                    , contract_address
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
    )
    
      --  SELECT * FROM swap_data

    
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
                  WHEN token = tokens[2] AND balance_change > int256 '0' THEN array[0x00, token, 0x00]
                  WHEN token = tokens[3] AND balance_change > int256 '0' THEN array[0x00, 0x00, token]
                  ELSE array[0x00, 0x00, 0x00]
            END AS tokens_in
            , CASE WHEN token = tokens[1] AND balance_change < int256 '0' THEN array[token, 0x00, 0x00]
                  WHEN token = tokens[2] AND balance_change < int256 '0' THEN array[0x00, token, 0x00]
                  WHEN token = tokens[3] AND balance_change < int256 '0' THEN array[0x00, 0x00, token]
                  ELSE array[0x00, 0x00, 0x00]
            END AS tokens_out
            , CASE WHEN token = tokens[1] AND balance_change > int256 '0' THEN array[abs(balance_change), int256 '0', int256 '0']
                  WHEN token = tokens[2] AND balance_change > int256 '0' THEN array[int256 '0', abs(balance_change), int256 '0']
                  WHEN token = tokens[3] AND balance_change > int256 '0' THEN array[int256 '0', int256 '0', abs(balance_change)]
                  ELSE array[int256 '0', int256 '0', int256 '0']
            END AS amounts_in
            , CASE WHEN token = tokens[1] AND balance_change < int256 '0' THEN array[abs(balance_change), int256 '0', int256 '0']
                  WHEN token = tokens[2] AND balance_change < int256 '0' THEN array[int256 '0', abs(balance_change), int256 '0']
                  WHEN token = tokens[3] AND balance_change < int256 '0' THEN array[int256 '0', int256 '0', abs(balance_change)]
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
    
  -- select * from pool_balance_managed
    
--     --select * from pool_balance_managed --where balance_change_is_value_transfer = False
    
    
    , join_exit_swap AS (
        SELECT
            action_type
            , evt_block_time AS block_time
            , evt_block_number AS block_number
            , evt_tx_hash AS tx_hash
            , tx_index AS tx_index
            , evt_index AS action_index
            , poolId AS pool_id
            , tokens_in
            , tokens_out
            , amounts_in
            , amounts_out
            , tx_from
            , tx_to
            , tokens
        FROM join_exit_call_data
        UNION ALL 
        SELECT
            action_type
            , evt_block_time AS block_time
            , evt_block_number AS block_number
            , evt_tx_hash AS tx_hash
            , tx_index AS tx_index
            , evt_index AS action_index
            , poolId AS pool_id
            , tokens_in
            , tokens_out
            , amounts_in
            , amounts_out
            , tx_from
            , tx_to
            , tokens
        FROM swap_data
        UNION ALL 
        SELECT
            action_type
            , evt_block_time AS block_time
            , evt_block_number AS block_number
            , evt_tx_hash AS tx_hash
            , NULL AS tx_index
            , evt_index AS action_index
            , poolId AS pool_id
            , tokens_in
            , tokens_out
            , amounts_in
            , amounts_out
            , NULL AS tx_from
            , NULL AS tx_to
            , tokens
        FROM pool_balance_managed
    )
    
  -- select * from join_exit_swap
    
--     , token_targets AS (
--         SELECT 
--             block_number AS bn_targets
--             , contract_address AS ca_targets
--             , topic0
--             , tx_hash AS th_targets
--             , index AS ai_targets
--             , bytearray_substring(topic1, 13, 20) AS target_token
--             , bytearray_to_uint256(bytearray_substring(data, 1, 32)) AS lower_target
--             , bytearray_to_uint256(bytearray_substring(data, 33)) AS upper_target
--         FROM evms.logs
--         WHERE blockchain  = '{{blockchain}}'
--         AND   topic0      = 0xd0e27a0d0c2cb09280fa5e4487315455b32afcdcf012dc35b6ef2a0e3c4d1280
--         AND block_number >= (SELECT min(evt_block_number) FROM sub_pools)
--     )
    
    , swap_fees AS (
        SELECT
            blockchain
            , block_number AS bn_swapfee
            , contract_address AS ca_swapfee
            , tx_hash AS th_swapfee
            , index AS ai_swapfee
            , swap_fee_percentage
        FROM balancer.pools_fees
        WHERE blockchain = '{{blockchain}}'
    )

    , join_exit_swap_targets_swapfees AS (
    -- Join the token targets and swap fees tables for each linear pool.
    -- Since both can change it is important to get the appropriate ...
    -- value given the position of a swap/join/exit in a block.
    -- When doing this for multiple tables they must be ordered identically.
        SELECT * 
            -- , ROW_NUMBER() OVER (
            --     PARTITION BY x.pool_id, x.tx_hash, x.action_index 
            --     ORDER BY 
            --         --t.bn_targets DESC
            --         --, t.ai_targets DESC
            --         f.bn_swapfee DESC
            --         , f.ai_swapfee DESC
            -- ) AS rn_targets
            , ROW_NUMBER() OVER (
                PARTITION BY x.pool_id, x.tx_hash, x.action_index 
                ORDER BY 
                    --t.bn_targets DESC
                    --, t.ai_targets DESC
                f.bn_swapfee DESC
                , f.ai_swapfee DESC
            ) AS rn_swap_fee
        FROM join_exit_swap x
        -- Token Targets
        -- LEFT JOIN token_targets t
        --     ON t.ca_targets = x.tokens[1] 
        --     AND ARRAY[t.bn_targets, t.ai_targets] < ARRAY[x.block_number, x.action_index]
        -- Pool Swap Fee
        LEFT JOIN swap_fees f
            ON f.ca_swapfee = x.tokens[1]
            AND ARRAY[f.bn_swapfee, f.ai_swapfee] < ARRAY[x.block_number, x.action_index]
    )
    
 --select * from join_exit_swap_targets_swapfees
    
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
            action_type
            , block_time
            , block_number
            , tx_hash
            , tx_index
            , action_index
            , pool_id
            , tokens_in
            , tokens_out
            , amounts_in
            , amounts_out
            , tx_from
            , tx_to
            , tokens
            -- , CAST(lower_target AS int256) AS lower_target -- Match data type in amounts in/out arrays
            -- , CAST(upper_target AS int256) AS upper_target -- ^
            , swap_fee_percentage AS swap_fee_raw
            , swap_fee_percentage/1e18 AS swap_fee
            , zip_with(
                amounts_in -- Array 1 length = 3
                , amounts_out -- Array 2 length = 3
                , (x, y) -> x - y -- Function x = entry in array 1, y = entry in array 2, perform operation x - y
            ) AS array_in_minus_out
            , ARRAY[
                amounts_in[1] / POWER(10, COALESCE(lp.d, 18))
                , amounts_in[2] / POWER(10, COALESCE(t2.d, t3.d, 18))
                , amounts_in[3] / POWER(10, COALESCE(t3.d, t2.d, 18))
            ] AS amounts_in_scaled
            , ARRAY[
                amounts_out[1] / POWER(10, COALESCE(lp.d, 18))
                , amounts_out[2] / POWER(10, COALESCE(t2.d, t3.d, 18))
                , amounts_out[3] / POWER(10, COALESCE(t3.d, t2.d, 18))
            ] AS amounts_out_scaled
            , lp.s -- token_1 (LP) symbol
            , COALESCE(lp.d, 18) AS lp_d -- tokne_1 (LP) decimals (18)
            , t2.s -- token_2 symbol
            , COALESCE(t2.d, t3.d, 18) AS t2_d
            , t3.s -- token_3 symbol
            , COALESCE(t3.d, t2.d, 18) AS t3_d
        FROM join_exit_swap_targets_swapfees x
        LEFT JOIN t_erc20 lp
            ON  lp.b = '{{blockchain}}' 
            AND lp.c = x.tokens[1]
        LEFT JOIN t_erc20 t2
            ON  t2.b = '{{blockchain}}' 
            AND t2.c = x.tokens[2]
        LEFT JOIN t_erc20 t3
            ON  t3.b = '{{blockchain}}' 
            AND t3.c = x.tokens[3]
        WHERE rn_swap_fee = 1
    )
    
    --SELECT * FROM data_table_1
    
--     , rebalanced AS (
--         SELECT * 
--             , desired_balance / POWER(10, t3_d) AS normalized_desired_balance
--             , CASE WHEN method_rebalance IS NOT NULL THEN 'rebalance'
--             ELSE action_type END AS a
--         FROM data_table_1 x
--         LEFT JOIN (
--             SELECT
--                 block_number AS bn
--                 , hash AS th
--                 , bytearray_substring(data, 1, 4) AS method_rebalance -- 0x6463a73e rebalance(address _pool, uint256 _desiredBalance, bool _useFlash)
--                 , bytearray_substring(data, 17, 20) AS pool_address
--                 , bytearray_to_int256(bytearray_substring(data, 37, 32)) AS desired_balance -- needs to be scaled using the main token decimals
--                 , IF(bytearray_to_int256(bytearray_substring(data, 69, 32)) = int256 '1', True, False) AS use_flash
--                 , (gas_price * gas_used)/1e18 AS txn_cost_in_eth
--             FROM evms.transactions 
--             WHERE blockchain  = '{{blockchain}}' 
--             AND   success     = True
--             AND bytearray_substring(data, 1, 4) = 0x6463a73e 
--             AND block_number >= (SELECT min(evt_block_number) FROM sub_pools)
            
--         ) rebal
--             ON  rebal.bn            = x.block_number
--             AND rebal.th            = x.tx_hash
--             AND rebal.pool_address  = x.tokens[1]
--     )
    
    , data_table_2 AS (
        SELECT --* 
            action_type
            , block_time
            , block_number
            , tx_hash
            , action_index
            , pool_id
            , tx_from
            , tokens_in
            , tokens_out
            , amounts_in
            , amounts_out
            , array_in_minus_out
            , amounts_in_scaled
            , amounts_out_scaled
            , sum(array_in_minus_out[1]) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC ROWS UNBOUNDED PRECEDING) AS lp_virtual_supply_raw
            , sum(array_in_minus_out[2]) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC ROWS UNBOUNDED PRECEDING) AS token_1_supply_raw
            , sum(array_in_minus_out[3]) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC ROWS UNBOUNDED PRECEDING) AS token_2_supply_raw
            , sum(array_in_minus_out[1] / POWER(10, lp_d)) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC ROWS UNBOUNDED PRECEDING) 
                AS lp_virtual_supply
            , sum(array_in_minus_out[2] / POWER(10, t2_d)) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC ROWS UNBOUNDED PRECEDING)
                AS token_1_supply
            , sum(array_in_minus_out[3] / POWER(10, t3_d)) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC ROWS UNBOUNDED PRECEDING)
                AS token_2_supply

            --, tokens
            -- , lower_target/1e18 AS lt
            -- , upper_target/1e18 AS up
            , swap_fee
            -- , txn_cost_in_eth
            -- , normalized_desired_balance
        FROM data_table_1
    )
    
      -- SELECT * FROM data_table_2

    
    , data_table_3 AS (
        SELECT *
            -- Get the previous supply amount for each token
            , LAG(lp_virtual_supply) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC) AS prev_lp_supply
            , LAG(token_1_supply) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC) AS prev_t1_supply
            , LAG(token_2_supply) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC) AS prev_t2_supply
        FROM data_table_2
    )
    
            --SELECT * FROM data_table_3

    
    -- , data_table_4 AS (
    --     SELECT *
    --         , IF(action_type = 'swap' OR action_type = 'swap_join' OR action_type = 'swap_join'
    --             , CASE
    --                 -- Calculate fee_in_USD for movement around Upper target
    --                 WHEN token_2_supply > up AND prev_t2_supply > up THEN (token_2_supply - prev_t2_supply) * swap_fee
    --                 WHEN token_2_supply > up AND prev_t2_supply < up THEN (token_2_supply - up) * swap_fee
    --                 WHEN token_2_supply < up AND prev_t2_supply > up THEN (prev_t2_supply - up) * -swap_fee
    --                 -- Calculate fee_in_USD for movement around Lower target
    --                 WHEN token_2_supply < lt AND prev_t2_supply < lt THEN (prev_t2_supply - token_2_supply) * swap_fee
    --                 WHEN token_2_supply < lt AND prev_t2_supply > lt THEN (lt - token_2_supply) * swap_fee
    --                 WHEN token_2_supply > lt AND prev_t2_supply < lt THEN (lt - prev_t2_supply) * -swap_fee
    --                 ELSE 0
    --             END
    --             , 0
    --         ) AS fee_in_USD
    --     FROM data_table_3
    -- )
    

SELECT 
    block_number
    -- , CASE WHEN txn_cost_in_eth IS NOT NULL THEN 'rebalance' 
    --       ELSE action_type 
    -- END AS action_type
    , action_type
    , action_index
    , tx_hash
    , tx_from AS trader
    , pool_id
    , tokens_in
    , tokens_out
    , amounts_in
    , amounts_out
    , lp_virtual_supply_raw
    , token_1_supply_raw
    , token_2_supply_raw
    , array_in_minus_out
    , amounts_in_scaled
    , amounts_out_scaled
    , lp_virtual_supply
    , token_1_supply
    , token_2_supply
    -- , lt
    -- , up
    , token_2_supply
    , prev_t2_supply
    -- , fee_in_usd
    -- , sum(fee_in_usd) OVER(PARTITION BY pool_id ORDER BY block_number ASC, action_index ASC ROWS UNBOUNDED PRECEDING) AS cumu_fee_in_usd
FROM data_table_3
ORDER BY block_number ASC, action_index ASC, pool_id
