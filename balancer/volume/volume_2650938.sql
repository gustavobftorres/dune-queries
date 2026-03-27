-- part of a query repo
-- query name: Volume
-- query link: https://dune.com/queries/2650938


SELECT
    SUM(CASE WHEN block_time >= NOW() - INTERVAL '1' DAY THEN amount_usd END) / 1e6 AS volume_1d,
    SUM(CASE WHEN block_time >= NOW() - INTERVAL '7' DAY THEN amount_usd END) / 1e6 AS volume_7d,
    SUM(CASE WHEN block_time >= NOW() - INTERVAL '30' DAY THEN amount_usd END) / 1e6 AS volume_30d
FROM balancer.trades
WHERE block_time >= NOW() - INTERVAL '30' DAY
AND ('{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}')
AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')