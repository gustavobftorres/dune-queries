-- part of a query repo
-- query name: Gauge ressurection via adaptor
-- query link: https://dune.com/queries/4538853


/*select contract_address, call_block_time, 'activate' from balancer_ethereum.CappedPolygonRootGauge_call_initialize
UNION
select contract_address, call_block_time, 'kill' from balancer_ethereum.CappedPolygonRootGauge_call_killGauge
UNION
*/
SELECT 
BYTEARRAY_SUBSTRING(topic3, 13, 20) AS target_gauge_7,
*
FROM ethereum.logs
WHERE contract_address = 0xf5dECDB1f3d1ee384908Fbe16D2F0348AE43a9eA
AND topic0 = 0xd4634f1cb58f0ea9cb6e1838192e5c3077115fcc17f0f6af3db4757114f42739
AND tx_hash = 0xe5fbcce56b3d1f6683ff9fa02bac836fa4adc051ba5a7991378e9247dc5953d4