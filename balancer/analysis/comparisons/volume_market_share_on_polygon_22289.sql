-- part of a query repo
-- query name: Volume Market Share on Polygon
-- query link: https://dune.com/queries/22289


SELECT 
    date_trunc('week', block_time) AS week,
    project,
    SUM(usd_amount) AS volume
FROM dex.trades 
WHERE block_time >= '{{2. Start date}}'
AND block_time <= '{{3. End date}}'
AND project IN ('Balancer', 'Sushiswap', 'Quickswap', 'Uniswap')
GROUP BY 1, 2
