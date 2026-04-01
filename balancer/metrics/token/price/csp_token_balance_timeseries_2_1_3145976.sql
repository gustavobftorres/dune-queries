-- part of a query repo
-- query name: CSP Token Balance Timeseries 2.1
-- query link: https://dune.com/queries/3145976


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
        FROM balancer_v2_ethereum.Vault_evt_TokensRegistered x
        WHERE 0xe7e2c68d3b13d905bbb636709cf4dfd21076b9d20000000000000000000005ca = x.poolId
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
            , ARRAY[
                CASE 
                    WHEN delta_pool_token > INT256 '0' THEN
                    CASE 
                        WHEN lp_token_burned > INT256 '0' THEN 0x00 
                        ELSE pool_token 
                    END
                    ELSE pool_token
                END
                , CASE WHEN delta_token_1 > INT256 '0' THEN token_1 ELSE 0x00 END
                , CASE WHEN delta_token_2 > INT256 '0' THEN token_2 ELSE 0x00 END
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
                , CASE WHEN delta_token_1 < INT256 '0' THEN token_1 ELSE 0x00 END
                , CASE WHEN delta_token_2 < INT256 '0' THEN token_2 ELSE 0x00 END
            ] AS tokens_out
    
            , ARRAY[
                CASE 
                    WHEN delta_pool_token > INT256 '0' THEN lp_token_minted
                    ELSE lp_token_minted
                END
                , CASE WHEN delta_token_1 >= INT256 '0' THEN delta_token_1 ELSE INT256 '0' END
                , CASE WHEN delta_token_2 >= INT256 '0' THEN delta_token_2 ELSE INT256 '0' END
            ] AS amounts_in
    
            , ARRAY[
                CASE 
                WHEN delta_pool_token > INT256 '0' THEN delta_pool_token + lp_token_burned
                    ELSE lp_token_burned
                END
                , CASE WHEN delta_token_1 < INT256 '0' THEN -1 * delta_token_1 ELSE INT256 '0' END
                , CASE WHEN delta_token_2 < INT256 '0' THEN -1 * delta_token_2 ELSE INT256 '0' END
            ] AS amounts_out
        
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
                -- , CASE WHEN sp.tokens[1] = x.tokens[1] THEN x.deltas[1]
                --       WHEN sp.tokens[1] = x.tokens[2] THEN x.deltas[2]
                --       WHEN sp.tokens[1] = x.tokens[3] THEN x.deltas[3]
                -- END AS delta_pool_token
                , x.deltas[ARRAY_POSITION(x.tokens, sp.pool_token)] AS delta_pool_token
                , CASE WHEN sp.tokens[2] = x.tokens[1] THEN x.deltas[1]
                       WHEN sp.tokens[2] = x.tokens[2] THEN x.deltas[2]
                       WHEN sp.tokens[2] = x.tokens[3] THEN x.deltas[3] 
                END AS delta_token_1
                , CASE WHEN sp.tokens[3] = x.tokens[1] THEN x.deltas[1]
                       WHEN sp.tokens[3] = x.tokens[2] THEN x.deltas[2]
                       WHEN sp.tokens[3] = x.tokens[3] THEN x.deltas[3]
                END AS delta_token_2
                , y.lp_token_burned
                , y.lp_token_minted
                , x.liquidityProvider
                , sp.tokens = x.tokens
                , sp.tokens
            FROM balancer_v2_ethereum.Vault_evt_PoolBalanceChanged x
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
                WHERE blockchain = 'ethereum'
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
            WHERE blockchain = 'ethereum'
            AND block_number >= (SELECT min(evt_block_number) FROM sub_pools)
        ) l
        ON l.block_number = x.evt_block_number
        AND l.tx_hash     = x.evt_tx_hash
    ) -- End CTE
    
   --SELECT * FROM join_exit_call_data WHERE evt_block_number = 17975866
    
    
    , swap_data AS (
            SELECT
                action_type
                , evt_block_time
                , evt_block_number
                , swaps.evt_tx_hash
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
                , CASE WHEN tokenIn = tokens[1] THEN array[lp_token_minted - amountIn, int256 '0', int256 '0']
                      WHEN tokenIn = tokens[2] THEN array[int256 '0', amountIn, int256 '0']
                      WHEN tokenIn = tokens[3] THEN array[int256 '0', int256 '0', amountIn]
                END AS amounts_in
                , CASE WHEN tokenOut = tokens[1] THEN array[lp_token_burned - amountOut, int256 '0', int256 '0']
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
                FROM balancer_v2_ethereum.Vault_evt_Swap x
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
                WHERE  blockchain = 'ethereum'
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
                WHERE blockchain = 'ethereum'
                    AND evt_block_number >= (SELECT min(evt_block_number) FROM sub_pools) --####################################
                    AND (to = 0x0000000000000000000000000000000000000000 OR "from" = 0x0000000000000000000000000000000000000000)
                    AND contract_address = (SELECT pool_token FROM sub_pools)
                GROUP BY 1, 2
            ) mint_burn
                ON mint_burn.evt_tx_hash = swaps.evt_tx_hash
    )
    
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
                FROM balancer_v2_ethereum.Vault_evt_PoolBalanceManaged z
                INNER JOIN (SELECT poolId, tokens FROM sub_pools) sp
                    ON sp.poolId = z.poolId
                WHERE cashDelta = int256 '0'
            ) x
            INNER JOIN (
                SELECT 
                    blockchain, "to", "from", contract_address, evt_index, evt_tx_hash, CAST(value AS int256) AS transfer_value 
                FROM evms.erc20_transfers
                WHERE blockchain = 'ethereum'
                AND evt_block_number >= (SELECT min(evt_block_number) FROM sub_pools) --###################################
                AND "to" = 0xba12222222228d8ba445958a75a0704d566bf2c8
                UNION ALL
                SELECT 
                    blockchain, "to", "from", contract_address, evt_index, evt_tx_hash, -CAST(value AS int256) AS transfer_value 
                FROM evms.erc20_transfers
                WHERE blockchain = 'ethereum'
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
    
    , swap_fees AS (
        SELECT
            blockchain
            , block_number AS bn_swapfee
            , contract_address AS ca_swapfee
            , tx_hash AS th_swapfee
            , index AS ai_swapfee
            , swap_fee_percentage
        FROM balancer.pools_fees
        WHERE blockchain = 'ethereum'
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
            ON  lp.b = 'ethereum' 
            AND lp.c = x.tokens[1]
        LEFT JOIN t_erc20 t2
            ON  t2.b = 'ethereum' 
            AND t2.c = x.tokens[2]
        LEFT JOIN t_erc20 t3
            ON  t3.b = 'ethereum' 
            AND t3.c = x.tokens[3]
        WHERE rn_swap_fee = 1
    )
    
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

SELECT 
    block_number
    , lp_virtual_supply_raw
    , lp_virtual_supply
    -- , CASE WHEN txn_cost_in_eth IS NOT NULL THEN 'rebalance' 
    --       ELSE action_type 
    -- END AS action_type
--    , action_type
--    , action_index
    , tx_hash
--    , tx_from AS trader
--    , pool_id
 --   , tokens_in
 --   , tokens_out
    , amounts_in
    , amounts_out
    , token_1_supply_raw
    , token_2_supply_raw
    , array_in_minus_out
    , amounts_in_scaled
    , amounts_out_scaled
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
