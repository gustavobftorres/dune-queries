-- part of a query repo
-- query name: kill gauges via AuthorizerAdaptorEntrypoint
-- query link: https://dune.com/queries/3065107


SELECT *
    , block_number AS kill_block_number
    , index AS kill_index
    , bytearray_substring(topic2, 13, 32) AS caller
    , bytearray_substring(topic3, 13, 32) AS target --root_gauge
FROM ethereum.logs
WHERE topic0 = 0xd4634f1cb58f0ea9cb6e1838192e5c3077115fcc17f0f6af3db4757114f42739
AND topic1 = 0xab8f094500000000000000000000000000000000000000000000000000000000
