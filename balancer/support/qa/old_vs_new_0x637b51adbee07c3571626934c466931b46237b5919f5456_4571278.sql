-- part of a query repo
-- query name: old vs. new 0x637b51adbee07c3571626934c466931b46237b5919f5456762ee9dadba5486a6
-- query link: https://dune.com/queries/4571278


SELECT FALSE AS is_updated,  * FROM balancer_v3_ethereum.base_trades
WHERE blockchain = 'ethereum'
AND tx_hash = 0x637b51adbee07c3571626934c466931b46237b5919f5456762ee9dadba5486a6

UNION

SELECT TRUE AS ia_updated,  * FROM test_schema.git_dunesql_bbdfa38_balancer_v3_ethereum_base_trades
WHERE blockchain = 'ethereum'
AND tx_hash = 0x637b51adbee07c3571626934c466931b46237b5919f5456762ee9dadba5486a6