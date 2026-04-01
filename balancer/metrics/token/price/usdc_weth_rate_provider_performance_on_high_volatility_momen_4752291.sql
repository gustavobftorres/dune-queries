-- part of a query repo
-- query name: USDC/WETH rate provider performance on high volatility moments
-- query link: https://dune.com/queries/4752291


WITH rate_updates AS (
    SELECT
        block_time,
        block_number,
        tx_hash,
        bytearray_to_uint256(output)/1e18 AS rate,
        price,
        (bytearray_to_uint256(output)/1e18 - price) / price AS diff_pct
    FROM {{Blockchain}}.traces
    LEFT JOIN prices.usd p 
        ON DATE_TRUNC('minute', block_time) = minute
        AND p.symbol = 'WETH'
        AND p.blockchain = 'ethereum'
    WHERE input = bytearray_substring(keccak(cast(concat('{{Function Name}}', '()') as varbinary)), 1, 4)
        AND to = {{Contract Address}}
        AND block_number >= {{Start Block}}
        AND success
),

flagged_cases AS (
    SELECT
        ru.block_time AS last_update_time,
        ru.block_number AS last_update_block,
        ru.tx_hash AS last_update_tx,
        ru.rate AS last_rate,
        ru.price AS last_price,
        ru.diff_pct AS last_diff_pct,
        LEAD(ru.block_time) OVER (ORDER BY ru.block_time) AS next_update_time,
        LEAD(ru.block_number) OVER (ORDER BY ru.block_time) AS next_update_block,
        LEAD(ru.tx_hash) OVER (ORDER BY ru.block_time) AS next_update_tx,
        LEAD(ru.rate) OVER (ORDER BY ru.block_time) AS next_rate,
        LEAD(ru.price) OVER (ORDER BY ru.block_time) AS next_price,
        LEAD(ru.diff_pct) OVER (ORDER BY ru.block_time) AS next_diff_pct
    FROM rate_updates ru
),

delayed_updates AS (
    SELECT
        *,
        date_diff('minute', last_update_time, next_update_time) AS minutes_since_last_update,
        next_price - last_price AS price_delta
    FROM flagged_cases
    WHERE 
        (ABS(last_diff_pct) >= 0.0015 OR date_diff('minute', last_update_time, next_update_time) >= 10)
)
SELECT 
    last_update_time,
    last_update_block,
    last_update_tx,
    last_rate,
    last_price,
    last_diff_pct,
    next_update_time,
    next_update_block,
    next_update_tx,
    next_rate,
    next_price,
    next_diff_pct,
    minutes_since_last_update,
    price_delta
FROM delayed_updates
WHERE price_delta > 5
ORDER BY minutes_since_last_update DESC;