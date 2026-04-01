-- part of a query repo
-- query name: Balancer Volume
-- query link: https://dune.com/queries/2829077


SELECT
    SUM(CASE WHEN block_time >= NOW() - INTERVAL '1' DAY  THEN amount_usd END) / 1e6 AS "Volume 1D",
    SUM(CASE WHEN block_time >= NOW() - INTERVAL '7' DAY  THEN amount_usd END) / 1e6 AS "Volume 7D",
    SUM(CASE WHEN block_time >= NOW() - INTERVAL '30' DAY THEN amount_usd END) / 1e6 AS "Volume 30D"
FROM balancer.trades
WHERE block_time >= NOW() - INTERVAL '30' DAY
  AND ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
  AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')