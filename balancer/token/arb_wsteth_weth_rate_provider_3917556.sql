-- part of a query repo
-- query name: Arb wsteth/weth rate provider
-- query link: https://dune.com/queries/3917556


SELECT
    block_time,
    block_number,
    get_href(
        get_chain_explorer_tx_hash(
            'arbitrum',
            tx_hash
        ),
        CAST(tx_hash AS VARCHAR)
    ) AS tx_hash,
    bytearray_to_uint256(output)/1e18 as rate
FROM arbitrum.traces
WHERE input = bytearray_substring(keccak(cast(concat('getRate', '()') as varbinary)), 1, 4)
AND to = 0xf7c5c26b574063e7b098ed74fad6779e65e3f836
AND block_number >= 21091291
AND success
ORDER BY 1
