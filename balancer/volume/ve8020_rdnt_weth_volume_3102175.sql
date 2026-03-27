-- part of a query repo
-- query name: ve8020 RDNT WETH Volume
-- query link: https://dune.com/queries/3102175


SELECT CAST(date_trunc('week',block_date) as timestamp) as day, sum(amount_usd) as volume,
(SELECT SUM(CASE WHEN block_time > now() - interval '24' hour THEN amount_usd ELSE 0 END)/1e3 FROM balancer.trades
WHERE project_contract_address = 0x32df62dc3aed2cd6224193052ce665dc18165841
AND blockchain = 'arbitrum') as volume_24h,
(SELECT SUM(CASE WHEN block_time > now() - interval '7' day THEN amount_usd ELSE 0 END)/1e6 FROM balancer.trades
WHERE project_contract_address = 0x32df62dc3aed2cd6224193052ce665dc18165841
AND blockchain = 'arbitrum') as volume_7d
FROM balancer.trades WHERE project_contract_address = 0x32df62dc3aed2cd6224193052ce665dc18165841
AND blockchain = 'arbitrum'
GROUP BY 1
ORDER BY 1 DESC