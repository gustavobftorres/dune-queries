-- part of a query repo
-- query name: CoW Share on Arbitrum (weekly)
-- query link: https://dune.com/queries/4079967


SELECT
    date_trunc('week', block_date) as block_date,
    SUM(amount_usd) AS total_volume,
    SUM(CASE WHEN tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41 THEN amount_usd ELSE 0 END) AS cow_volume,
    SUM(CASE WHEN tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41 THEN amount_usd ELSE 0 END) /
    SUM(amount_usd) AS cow_share
FROM dex.trades
WHERE blockchain = 'arbitrum'
AND block_month >= TIMESTAMP '2024-04-01 00:00' --CoW launch on ARB
GROUP BY 1
ORDER BY 1 DESC
