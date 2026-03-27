-- part of a query repo
-- query name: v3_vault_bpt_supply
-- query link: https://dune.com/queries/3851357


 WITH transfers AS (
        SELECT
            block_date AS day,
            contract_address AS token,
            COALESCE(SUM(CASE WHEN t."from" = 0x0000000000000000000000000000000000000000 THEN value / POWER(10, 18) ELSE 0 END), 0) AS mints,
            COALESCE(SUM(CASE WHEN t.to = 0x0000000000000000000000000000000000000000 THEN value / POWER(10, 18) ELSE 0 END), 0) AS burns
        FROM query_3851356 t
        GROUP BY 1, 2
    ),

    -- Calculate token balances over time
    balances AS (
        SELECT
            day,
            token,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY token ORDER BY DAY) AS day_of_next_change,
            SUM(COALESCE(mints, 0) - COALESCE(burns, 0)) OVER (PARTITION BY token ORDER BY DAY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS supply
        FROM transfers
    ),

    -- Calculating Joins(mint) and Exits(burn) via Swap
    joins AS (
        SELECT 
            DATE_TRUNC('day', evt_block_time) AS block_date, 
            tokenOut,
            pool_type,
            SUM(amountOut / POWER(10, 18)) AS ajoins
        FROM balancer_testnet_sepolia.Vault_evt_Swap 
        LEFT JOIN pool_labels ON BYTEARRAY_SUBSTRING(poolId, 1, 20) = address
        WHERE tokenOut = BYTEARRAY_SUBSTRING(poolId, 1, 20)       
        GROUP BY 1, 2, 3
    ),

    exits AS (
        SELECT 
            DATE_TRUNC('day', evt_block_time) AS block_date, 
            tokenIn,
            pool_type,
            SUM(amountIn / POWER(10, 18)) AS aexits
        FROM balancer_testnet_sepolia.Vault_evt_Swap 
        LEFT JOIN pool_labels ON BYTEARRAY_SUBSTRING(poolId, 1, 20) = address
        WHERE tokenIn = BYTEARRAY_SUBSTRING(poolId, 1, 20)        
        GROUP BY 1, 2, 3
    ),

    joins_and_exits AS (
        SELECT 
            j.block_date, 
            j.tokenOut AS bpt, 
            SUM(COALESCE(ajoins, 0) - COALESCE(aexits, 0)) OVER (PARTITION BY j.tokenOut ORDER BY j.block_date ASC) AS adelta
        FROM joins j
        FULL OUTER JOIN exits e ON j.block_date = e.block_date AND e.tokenIn = j.tokenOut
    ),

    calendar AS (
        SELECT 
            date_sequence AS day
        FROM unnest(sequence(date('2024-04-21'), date(now()), interval '1' day)) as t(date_sequence)
    )

    SELECT
        c.day,
        l.pool_type,
        '3' as version,
        'sepolia' as blockchain,
        b.token AS token_address,
        COALESCE(SUM(b.supply + COALESCE(adelta, 0)),0) AS supply
    FROM calendar c 
    LEFT JOIN balances b ON b.day <= c.day AND c.day < b.day_of_next_change
    LEFT JOIN joins_and_exits j ON c.day = j.block_date AND b.token = j.bpt
    LEFT JOIN premints p ON b.token = p.bpt
    LEFT JOIN pool_labels l ON b.token = l.address
    GROUP BY 1, 2, 3, 4, 5
    HAVING SUM(b.supply + COALESCE(adelta, 0)) >= 0  --simple filter to remove outliers