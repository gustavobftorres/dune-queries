-- part of a query repo
-- query name: Velodrome Slipstream Volume and Fees
-- query link: https://dune.com/queries/3630153


WITH erc20 AS ( 
    SELECT 
        contract_address AS token, 
        symbol, 
        decimals
    FROM tokens.erc20
    WHERE blockchain = 'optimism'
),
cl_pools AS (
    SELECT 
        pool, 
        token0, 
        token1,
       tickSpacing
    FROM velodrome_v2_optimism.CLFactory_evt_PoolCreated
),

pools AS (
    SELECT cl.pool,
        CONCAT(CONCAT('CL', CAST(tickSpacing AS varchar), '-'), t0.symbol, '/', t1.symbol) AS name,
        t0.decimals AS decimals0,
        t1.decimals AS decimals1,
        cl.token0,
        cl.token1,
        tickSpacing,
        CASE
            WHEN tickSpacing = 1 THEN 0.01 * 0.01
            WHEN tickSpacing = 50 THEN 0.05 * 0.01
            WHEN tickSpacing = 100 THEN 0.05 * 0.01
            WHEN tickSpacing = 200 THEN 0.3 * 0.01
            ELSE 1 * 0.01
        END AS fee
    FROM cl_pools cl
    INNER JOIN erc20 t0 ON t0.token = cl.token0
    INNER JOIN erc20 t1 ON t1.token = cl.token1),
    
daily_price AS (
    SELECT 
        DATE_TRUNC('day', evt_block_time) AS day,
        token, 
        AVG(price) / 1e18 AS price
    FROM velodrome_v2_optimism.PriceFetcher_evt_PriceFetched
    WHERE price != 0
    GROUP BY 1, 2
),

velo_daily_volume AS (
    SELECT 
        day, 
        pool_type, 
        contract_address, 
        sender,
        SUM(volume_usd) AS total_volume, 
        SUM(fee_usd) AS total_fees
    FROM (
        SELECT 
            t0.day, 
            name, 
            contract_address,  
            sender,
            pool_type,
            SUM(
                IF(amount0 > 0, amount0 / POWER(10, decimals0) * p0.price , 0) + 
                IF(amount1 > 0, amount1 / POWER(10, decimals1) * p1.price , 0)
            ) AS volume_usd,
            SUM(
                IF(amount0 > 0, amount0 / POWER(10, decimals0) * p0.price , 0) + 
                IF(amount1 > 0, amount1 / POWER(10, decimals1) * p1.price , 0)
            ) * fee AS fee_usd 
        FROM (
            SELECT 
                DATE_TRUNC('day', evt_block_time) AS day,
                contract_address,
                sender,
                SUM(IF(amount0 > 0, amount0, 0)) AS amount0, 
                SUM(IF(amount1 > 0, amount1, 0)) AS amount1,
                'cl' AS pool_type
            FROM velodrome_v2_optimism.CLPool_evt_Swap
            GROUP BY DATE_TRUNC('day', evt_block_time), contract_address, sender   
        ) t0 
        INNER JOIN pools t1 ON t0.contract_address = t1.pool
        INNER JOIN daily_price p0 ON p0.token = t1.token0 AND p0.day = t0.day
        INNER JOIN daily_price p1 ON p1.token = t1.token1 AND p1.day = t0.day
        GROUP BY 1, 2, contract_address, fee, pool_type, sender
        HAVING SUM(
            IF(amount0 > 0, amount0 / POWER(10, decimals0) * p0.price , 0) + 
            IF(amount1 > 0, amount1 / POWER(10, decimals1) * p1.price , 0)
        ) > 1000
    ) 
    GROUP BY 1, 2, contract_address ,sender
)

SELECT  
    day, 
    contract_address,
    sender,
    total_volume AS amount_usd,
    total_fees AS fees_usd
FROM velo_daily_volume
