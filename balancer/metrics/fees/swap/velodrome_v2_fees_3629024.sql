-- part of a query repo
-- query name: Velodrome V2 Fees
-- query link: https://dune.com/queries/3629024


WITH daily_fees AS (
SELECT 
    DATE_TRUNC('day', evt_block_time) AS day, 
    pair_type || 'AMM-' || token0_symbol || '/' || token1_symbol as pair, 
    pool,
    token0, 
    token1,
    SUM(total_fees_usd) AS total_fees_usd
    FROM (
    SELECT
    FLOOR( CAST( DATE_DIFF('second', v.evt_block_time, NOW()) as DOUBLE) / CAST(60*60*24 as DOUBLE) ) AS day_num,
    e0.symbol AS token0_symbol,
    e1.symbol AS token1_symbol,
    COALESCE((CAST(amount0 as DOUBLE)/CAST(POWER(10,e0.decimals) as DOUBLE))*CAST(COALESCE(pr0.price,p0.median_price) as DOUBLE) , 0.0) AS fees_token0_usd,
    COALESCE((CAST(amount1 as DOUBLE)/CAST(POWER(10,e1.decimals) as DOUBLE))*CAST(COALESCE(pr1.price,p1.median_price) as DOUBLE) , 0.0) AS fees_token1_usd,
    COALESCE((CAST(amount0 as DOUBLE)/CAST(POWER(10,e0.decimals) as DOUBLE))*CAST(COALESCE(pr0.price,p0.median_price) as DOUBLE) , 0.0)
    + COALESCE((CAST(amount1 as DOUBLE)/CAST(POWER(10,e1.decimals) as DOUBLE))*CAST(COALESCE(pr1.price,p1.median_price) as DOUBLE) , 0.0)
    AS total_fees_usd,
    v.contract_address AS pool, 
    token0, 
    token1, 
    v.evt_tx_hash, 
    v.evt_block_time,
    pair_type
    FROM (
        SELECT 
            fee.evt_block_time, 
            fee.contract_address, 
            fee.amount0, 
            fee.amount1, 
            pc.token0, 
            pc.token1, 
            fee.evt_tx_hash,
            'v2: ' || CASE WHEN stable = true THEN 's' ELSE 'v' END as pair_type
        FROM velodrome_v2_optimism.Pool_evt_Fees fee
        INNER JOIN velodrome_v2_optimism.PoolFactory_evt_PoolCreated pc
            ON fee.contract_address = pc.pool
        ) v
    LEFT JOIN prices.usd pr0
        ON pr0.contract_address = v.token0
        AND pr0.blockchain = 'optimism'
        AND pr0.minute = DATE_TRUNC('minute',v.evt_block_time)
    LEFT JOIN prices.usd pr1
        ON pr1.contract_address = v.token1
        AND pr1.blockchain = 'optimism'
        AND pr1.minute = DATE_TRUNC('minute',v.evt_block_time)
    LEFT JOIN dex.prices p0
        ON p0.contract_address = v.token0
        AND p0.blockchain = 'optimism'
        AND p0.hour = DATE_TRUNC('hour',v.evt_block_time)
        AND pr0.price is null
    LEFT JOIN dex.prices p1
        ON p1.contract_address = v.token1
        AND p1.blockchain = 'optimism'
        AND p1.hour = DATE_TRUNC('hour',v.evt_block_time)
        AND pr1.price is null
    LEFT JOIN tokens.erc20 e0
        ON e0.blockchain = 'optimism'
        AND e0.contract_address = v.token0
    LEFT JOIN tokens.erc20 e1
        ON e1.blockchain = 'optimism'
        AND e1.contract_address = v.token1) a
    GROUP BY 1, 2, 3, 4, 5
)

SELECT 
    day, 
    pair, 
    d.pool, 
    token0, 
    token1, 
    m.pool_type, 
    total_fees_usd 
FROM daily_fees d
LEFT JOIN query_3629980 m ON d.pool = m.pool AND m.project = 'velodrome'