-- part of a query repo
-- query name: EURe Historical Exchange Rates (Rate Provider vs. Actual)
-- query link: https://dune.com/queries/4841954


SELECT
    block_time,
    block_number,
    get_href(
        get_chain_explorer_tx_hash(
            'gnosis',
            tx_hash
        ),
        CAST(tx_hash AS VARCHAR)
    ) AS tx_hash,
    bytearray_to_uint256(output)/1e18 as rate,
    p.price AS actual_price,
    bytearray_to_uint256(output)/1e18 - p.price as diff,
    CASE 
        WHEN bytearray_to_uint256(output)/1e18 = p.price THEN NULL 
        ELSE 1
    END AS flag
FROM gnosis.traces t
JOIN prices.usd p ON DATE_TRUNC('minute', t.block_time) = p.minute
AND p.symbol = 'EURe'
WHERE input = bytearray_substring(keccak(cast(concat('getRate', '()') as varbinary)), 1, 4)
AND to = 0xe7511f6e5c593007ea8a7f52af4b066333765e03
AND block_number >= 27672404
AND success
ORDER BY 1
