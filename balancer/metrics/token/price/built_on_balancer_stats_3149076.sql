-- part of a query repo
-- query name: Built on Balancer Stats
-- query link: https://dune.com/queries/3149076


SELECT SUM(CASE WHEN block_time > now() - interval '24' hour THEN amount_usd ELSE 0 END)/1e6 as volume_24h,
SUM(CASE WHEN block_time > now() - interval '7' day THEN amount_usd ELSE 0 END)/1e6 as volume_7d,
SUM(CASE WHEN block_time > now() - interval '30' day THEN amount_usd ELSE 0 END)/1e6 as volume_30d,
SUM(amount_usd)/1e9 as volume_agg,
(SELECT SUM(tvl)/1e6
FROM query_3147646) as tvl
FROM balancer.trades t
LEFT JOIN query_3144841 q ON t.project_contract_address = BYTEARRAY_SUBSTRING(q.poolId,1,20) AND t.blockchain = q.blockchain
WHERE q.project IS NOT NULL 