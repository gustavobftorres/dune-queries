-- part of a query repo
-- query name: USDC/WETH rate provider check
-- query link: https://dune.com/queries/4745987


SELECT
    block_time,
    LEAD(block_time) OVER (ORDER BY block_time) AS next_block_time,
    date_diff('minute', block_time, LEAD(block_time) OVER (ORDER BY block_time)) AS time_between_tx,
    CASE WHEN date_diff('minute', block_time, LEAD(block_time) OVER (ORDER BY block_time)) > 20
    THEN TRUE
    ELSE FALSE
    END AS more_than_10_min_lag,
    block_number,
    get_href(
        get_chain_explorer_tx_hash(
            '{{Blockchain}}',
            tx_hash
        ),
        CAST(tx_hash AS VARCHAR)
    ) AS tx_hash,
    bytearray_to_uint256(output)/1e18 as rate,
    price,
    LEAD(price) OVER (ORDER BY block_time) AS next_price,
    price - LEAD(price) OVER (ORDER BY block_time) AS price_diff,
    (price - LEAD(price) OVER (ORDER BY block_time)) / LEAD(price) OVER (ORDER BY block_time) AS price_diff_pct,
    bytearray_to_uint256(output)/1e18 - price AS diff,
    (bytearray_to_uint256(output)/1e18 - price) / price AS diff_pct
FROM {{Blockchain}}.traces
LEFT JOIN prices.usd p ON DATE_TRUNC('minute', block_time) = minute
AND p.symbol = 'WETH'
AND p.blockchain = 'ethereum'
WHERE input = bytearray_substring(keccak(cast(concat('{{Function Name}}', '()') as varbinary)), 1, 4)
AND to = {{Contract Address}}
AND block_number >= {{Start Block}}
AND success
ORDER BY 1
