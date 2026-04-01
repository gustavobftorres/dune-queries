-- part of a query repo
-- query name: Historical Exchange Rates
-- query link: https://dune.com/queries/3537355


SELECT
    block_time,
    block_number,
    get_href(
        get_chain_explorer_tx_hash(
            '{{Blockchain}}',
            tx_hash
        ),
        CAST(tx_hash AS VARCHAR)
    ) AS tx_hash,
    bytearray_to_uint256(output)/1e18 as rate,
    CASE 
        WHEN output >= COALESCE(LAG(output) OVER (ORDER BY block_number, trace_address), 0x00) THEN 'UP' 
        ELSE 'DOWN'
    END AS diff,
    CASE 
        WHEN output >= COALESCE(LAG(output) OVER (ORDER BY block_number, trace_address), 0x00) THEN NULL 
        ELSE 1
    END AS flag
FROM {{Blockchain}}.traces
WHERE input = bytearray_substring(keccak(cast(concat('{{Function Name}}', '()') as varbinary)), 1, 4)
AND to = {{Contract Address}}
AND block_number >= {{Start Block}}
AND success
ORDER BY 1
