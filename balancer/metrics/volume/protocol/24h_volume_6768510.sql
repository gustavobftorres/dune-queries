-- part of a query repo
-- query name: 24h Volume
-- query link: https://dune.com/queries/6768510


SELECT SUM(amount_usd) / 1e6 AS "Volume on Balancer"
FROM balancer.trades
WHERE block_time >= NOW() - INTERVAL '1' DAY
  AND ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')