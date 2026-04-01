-- part of a query repo
-- query name: CoW Share on Arbitrum, Ethereum and Gnosis
-- query link: https://dune.com/queries/4074245


WITH data AS(
SELECT
    block_month,
    blockchain,
    SUM(amount_usd) AS total_volume,
    SUM(CASE WHEN tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41 THEN amount_usd ELSE 0 END) AS cow_volume,
    SUM(CASE WHEN tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41 THEN amount_usd ELSE 0 END) /
    SUM(amount_usd) AS cow_share
FROM dex.trades
WHERE blockchain IN ('arbitrum', 'gnosis', 'ethereum')
AND block_month >= TIMESTAMP '2023-01-01 00:00'
GROUP BY 1, 2
ORDER BY 1 DESC , 3 DESC)

SELECT * FROM data
WHERE cow_share IS NOT NULL
