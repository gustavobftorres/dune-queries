-- part of a query repo
-- query name: 0x8dd4df4ce580b9644437f3375e54f1ab09808228 bpt to address
-- query link: https://dune.com/queries/4583763




        SELECT
            "to", COUNT(*)
        FROM erc20_gnosis.evt_transfer t
            WHERE t."from" = 0x0000000000000000000000000000000000000000
            --AND t."to" = 0xce88686553686DA562CE7Cea497CE749DA109f9F
            AND contract_address = 0x8dd4df4ce580b9644437f3375e54f1ab09808228
        GROUP BY 1
