-- part of a query repo
-- query name: ve8020 Pools Stats - Volume (24h, 7D, 30D)
-- query link: https://dune.com/queries/6773608


SELECT
    SUM(CASE WHEN block_time > now() - interval '24' hour THEN amount_usd ELSE 0 END) / 1e6 AS volume_24h,
    SUM(CASE WHEN block_time > now() - interval '7'  day  THEN amount_usd ELSE 0 END) / 1e6 AS volume_7d,
    SUM(CASE WHEN block_time > now() - interval '30' day  THEN amount_usd ELSE 0 END) / 1e6 AS volume_30d
FROM balancer.trades