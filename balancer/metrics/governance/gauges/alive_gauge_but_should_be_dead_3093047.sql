-- part of a query repo
-- query name: alive gauge but should be dead
-- query link: https://dune.com/queries/3093047


-- All traces that include "kill" in function name. Would include unkill.
-- SELECT * FROM ethereum.traces_decoded WHERE to = 0x45bf48d996d22afc9bc150df7fb4d13a49088602 AND function_name LIKE '%kill%'

-- No unkill call
--SELECT * FROM balancer_ethereum.PolygonZkEVMRootGauge_call_unkillGauge WHERE contract_address = 0x45bf48d996d22afc9bc150df7fb4d13a49088602

SELECT * FROM ethereum.traces_decoded WHERE tx_hash = 0xb45361a8ec0d6f4e397e9463f74045800e2fac8b79f8fd58a99088c711c75f35
order by trace_address