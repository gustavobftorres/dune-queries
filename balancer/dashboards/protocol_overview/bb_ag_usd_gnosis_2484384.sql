-- part of a query repo
-- query name: bb-ag-USD (GNOSIS)
-- query link: https://dune.com/queries/2484384


WITH
    calendar as (
        WITH   
          -- 1 Min Calendar 
            "1 MIN" AS (
                SELECT
                    date_add('minute', step, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in DAYS}}' day) AS TIMESTAMP)) - INTERVAL '1' minute),
                            (date_trunc('minute', cast(now() AS TIMESTAMP)) + INTERVAL '1' minute),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 1440, 1)) AS t(step) )
                WHERE date_add('minute', step, day) <= date_trunc('minute', cast(now() AS TIMESTAMP))
            ),
            -- 5 Min Calendar 
            "5 MIN" AS (
                SELECT
                    date_add('minute', step * 5, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in DAYS}}' day) AS TIMESTAMP)) - INTERVAL '5' minute),
                            (date_trunc('minute', cast(now() AS TIMESTAMP)) + INTERVAL '5' minute),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 288, 1)) AS t(step) )
                WHERE date_add('minute', step * 5, day) <= date_trunc('minute', cast(now() AS TIMESTAMP))
            ),
            -- 15 Min Calendar 
            "15 MIN" AS (
                SELECT
                    date_add('minute', step * 15, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in DAYS}}' day) AS TIMESTAMP)) - INTERVAL '15' minute),
                            (date_trunc('minute', cast(now() AS TIMESTAMP)) + INTERVAL '15' minute),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 96, 1)) AS t(step) )
                WHERE date_add('minute', step * 15, day) <= date_trunc('minute', cast(now() AS TIMESTAMP))
            ),
            -- 30 Min Calendar 
            "30 MIN" AS (
                SELECT
                    date_add('minute', step * 30, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in DAYS}}' day) AS TIMESTAMP)) - INTERVAL '30' minute),
                            (date_trunc('minute', cast(now() AS TIMESTAMP)) + INTERVAL '30' minute),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 48, 1)) AS t(step) )
                WHERE date_add('minute', step * 30, day) <= date_trunc('minute', cast(now() AS TIMESTAMP))
            ),
            -- 1 Hour calendar
            "1 HOUR" AS (
                SELECT
                    date_add('hour', step, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in DAYS}}' day) AS TIMESTAMP)) - INTERVAL '1' hour),
                            (date_trunc('hour', cast(now() AS TIMESTAMP)) + INTERVAL '1' hour),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 24, 1)) AS t(step) )
                WHERE date_add('hour', step, day) <= date_trunc('hour', cast(now() AS TIMESTAMP))
            ),
            -- 2 Hour calendar
            "2 HOUR" AS (
                SELECT
                    date_add('hour', step * 2, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in DAYS}}' day) AS TIMESTAMP)) - INTERVAL '2' hour),
                            (date_trunc('hour', cast(now() AS TIMESTAMP)) + INTERVAL '2' hour),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM  UNNEST(SEQUENCE(1, 12, 1)) AS t(step) )
                WHERE date_add('hour', step * 2, day) <= date_trunc('hour', cast(now() AS TIMESTAMP))
            ),
            -- 4 Hour calendar
            "4 HOUR" AS (
                SELECT
                    date_add('hour', step * 4, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in DAYS}}' day) AS TIMESTAMP)) - INTERVAL '4' hour),
                            (date_trunc('hour', cast(now() AS TIMESTAMP)) + INTERVAL '4' hour),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 6, 1)) AS t(step) )
                WHERE date_add('hour', step * 4, day) <= date_trunc('hour', cast(now() AS TIMESTAMP))
            ),
            -- 1 Day calendar
            "1 DAY" AS (
                SELECT
                    date_add('day', step, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in DAYS}}' day) AS TIMESTAMP)) - INTERVAL '1' day),
                            (date_trunc('day', cast(now() AS TIMESTAMP)) + INTERVAL '1' day),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 1, 1)) AS t(step) )
                WHERE date_add('day', step, day) <= date_trunc('day', cast(now() AS TIMESTAMP))
            )
        
        SELECT * FROM "{{Interval}}"
    ),
-----------------------------------------------------------------------------------
    -- Take the parameter input for Pool Ids and turn it into a single column table
    pool_id_output AS (
        WITH 
            input AS (SELECT array[
                0x41211bba6d37f5a74b22e667533f080c7c7f3f1300000000000000000000000b, 0xd16f72b02da5f51231fde542a8b9e2777a478c8800000000000000000000000f, 0xe7f88d7d4ef2eb18fcf9dd7216ba7da1c46f3dd600000000000000000000000a --- CHANGEME
                ] AS pool_ids)
        SELECT pool_id_output.pool_ids
        FROM input
        CROSS JOIN UNNEST(input.pool_ids) AS pool_id_output(pool_ids)
    ),
    
    
-----------------------------------------------------------------------------------
    linear_pool_tokens AS (
        WITH
            get_info AS (
                SELECT poolId, tokens
                FROM (
                    select *, 
                        ROW_NUMBER() OVER(PARTITION BY poolId, evt_tx_hash ORDER BY evt_block_number DESC) AS latest_update
                    FROM balancer_v2_gnosis.Vault_evt_TokensRegistered
                    WHERE poolId in (TABLE pool_id_output)
                )
                WHERE latest_update = 1
            )
            SELECT gi.poolId, pt.tokens, pt.n
            FROM get_info gi
            CROSS JOIN UNNEST(gi.tokens) WITH ORDINALITY AS pt (tokens, n)
    ),
    base_tokens AS (
        SELECT tokens FROM (
            SELECT *,
                CASE WHEN SUBSTRING(CAST(poolId AS VARCHAR),1,42) = CAST(tokens AS VARCHAR) THEN TRUE ELSE FALSE
                END AS linear_pool_token
            FROM linear_pool_tokens
        ) WHERE linear_pool_token = FALSE
    ),
    linear_pool_addresses AS (
        SELECT tokens FROM (
            SELECT *,
                CASE WHEN SUBSTRING(CAST(poolId AS VARCHAR),1,42) = CAST(tokens AS VARCHAR) THEN TRUE ELSE FALSE
                END AS linear_pool_token
            FROM linear_pool_tokens
        ) WHERE linear_pool_token = TRUE
    ),
 -----------------------------------------------------------------------------------  
    -- Create a table that is the Linear Pool Address of the Pool Id(s) ... could be in same table possibly later
    
    --linear_pool_addresses AS (SELECT substring(CAST(pool_ids AS VARCHAR), 1, 42) AS linear_pool FROM pool_id_output),
    
    -- Grab the Linear Pool(s) parameters via the inputted Pool Id(s)
    pool_params AS (SELECT * FROM query_2407001 WHERE blockchain = 'gnosis' AND linear_pool in (TABLE linear_pool_addresses)), -- ############# BLOCKCHAIN ################## !!
    -- Create a data subset where we get swaps from the Vault filtered by the inputted pool id(s)
    subset as (SELECT evt_block_time, poolId, tokenIn, tokenOut, amountIn, amountOut FROM balancer_v2_gnosis.Vault_evt_Swap WHERE poolId in (TABLE pool_id_output)), -- ############# BLOCKCHAIN ################## !!
    -- Create a data subset where we get Balance changes by the pool manager from the Vault filtered by the inputted pool id(s)
    subset_2 as (SELECT evt_block_time, poolId, token, cashDelta, managedDelta FROM balancer_v2_gnosis.Vault_evt_PoolBalanceManaged WHERE poolId in (TABLE pool_id_output)), -- ############# BLOCKCHAIN ################## !!
    -- Get token balance changes base on swaps
    swaps_changes AS (
        SELECT
            date_trunc('minute', evt_block_time) AS minute,
            poolId AS pool_id,
            tokenIn AS token,
            cast(sum(amountIn) AS DOUBLE) AS delta
        FROM subset
        GROUP BY 1,2,3
        UNION all
        SELECT
            date_trunc('minute', evt_block_time) AS minute,
            poolId AS pool_id,
            tokenOut AS token,
            - cast(sum(amountOut) AS DOUBLE) AS delta
        FROM subset
        GROUP BY 1,2,3
    ),
    swaps_changes_2 AS (
        SELECT
            minute,
            pool_id, 
            token, 
            sum(delta) AS delta
        FROM swaps_changes
        GROUP BY 1,2,3
    ),
    management_changes AS (
        SELECT
            date_trunc('minute', evt_block_time) AS minute,
            poolId AS pool_id,
            token,
            cast(sum(cashDelta) AS DOUBLE) AS delta
        FROM subset_2
        GROUP BY 1,2,3
        UNION all
        SELECT
            date_trunc('minute', evt_block_time) AS minute,
            poolId AS pool_id,
            token,
            cast(sum(managedDelta) AS DOUBLE) AS delta
        FROM subset_2
        GROUP BY 1,2,3
    ),
    
    management_changes_2 AS (
        SELECT
            minute,
            pool_id, 
            token, 
            sum(delta) AS delta
        FROM management_changes
        GROUP BY 1,2,3
    ),
    
    all_changes AS (
        SELECT
            minute,
            pool_id,
            token,
            SUM(COALESCE(delta, 0)) AS delta
        FROM (
            SELECT * FROM swaps_changes_2
            UNION ALL
            SELECT * FROM management_changes_2
        ) changes
        WHERE token in (SELECT token FROM pool_params)
        GROUP BY 1,2,3 
    ),
    cumulative_balance_with_gaps AS (
            SELECT
                minute,
                pool_id,
                token,
                LEAD(minute, 1, NOW()) OVER (
                    PARTITION BY token, pool_id ORDER BY minute ) AS time_of_next_change,
                SUM(delta) OVER ( PARTITION BY pool_id, token
                    ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_amount
            FROM
                all_changes
    ),

    cumulative_balance AS (
        SELECT
            c.minute,
            b.pool_id,
            b.token,
            cumulative_amount / pow(10, t.decimals) AS amount,
            t.symbol,
            p.lowerTarget AS lt,
            p.upperTarget AS ut
        FROM
            calendar c
        LEFT JOIN cumulative_balance_with_gaps b ON b.minute <= c.minute AND c.minute < b.time_of_next_change
        LEFT JOIN tokens.erc20 t ON t.contract_address = b.token
        LEFT JOIN pool_params p ON CAST(p.linear_pool AS VARCHAR) = substring(CAST(b.pool_id AS VARCHAR), 1, 42)
        WHERE cast(b.token AS VARCHAR) != SUBSTRING(cast(b.pool_id AS VARCHAR), 1, 20)
        AND t.blockchain = 'gnosis' -- ############# BLOCKCHAIN ################## !!
    ),
    
    oneinchtrades AS (
        SELECT 
            date_trunc('minute', block_time) AS time,
            project_contract_address AS pool_id,
            count(*) as volume
        FROM dex.trades 
        WHERE blockchain = 'gnosis' 
            AND project = 'balancer'
            AND (token_bought_address in (TABLE linear_pool_addresses) OR token_sold_address in (TABLE linear_pool_addresses))
            AND token_bought_address NOT IN (TABLE base_tokens)
            AND token_sold_address NOT IN (TABLE base_tokens)
            AND block_time > date_trunc('day', now()) - interval '{{Time Frame in DAYS}}' day
            AND tx_to in (0x1111111254EEB25477B68fb85Ed929f73A960582, 0x1111111254fb6c44bac0bed2854e76f90643097d) -- 1inch v5, v4
        GROUP BY 1,2
    ),
    
    cal_gaps AS (
        SELECT *, LEAD(minute, 1, NOW()) OVER (ORDER BY minute) AS time_of_next_change FROM calendar
    ),
    
    oneinchtrades_2 AS (
        SELECT * FROM cal_gaps c LEFT JOIN oneinchtrades d ON d.time >= c.minute AND c.time_of_next_change > d.time
    ),
    
    oneinchtrades_3 AS (
        SELECT minute, pool_id, COALESCE(SUM(volume), 0) AS swaps_count FROM oneinchtrades_2 GROUP BY 1,2
    ),
    
    final AS (SELECT a.minute AS minute, pool_id, b.symbol AS token, amount, lt, ut
        FROM cumulative_balance a
        INNER JOIN tokens.erc20 b
        ON a.token = b.contract_address
    ),
    
    f_1 AS (SELECT minute, pool_id, token, amount, 2 AS order_num FROM final),
    f_2 AS (SELECT minute, pool_id, concat('UT: ', token) AS token, ut AS amount, 3 AS order_num FROM final),
    f_3 AS (SELECT minute, pool_id, concat('LT: ', token) AS token, lt AS amount, 4 AS order_num FROM final),
    f_4 AS (SELECT minute, pool_id, 'TRADES' AS token, swaps_count AS amount, 1 AS order_num FROM oneinchtrades_3),
    f_5 AS (SELECT * FROM f_1 UNION SELECT * FROM f_2 UNION SELECT * FROM f_3 UNION SELECT * FROM f_4)
    
SELECT * FROM f_5 ORDER BY order_num ASC