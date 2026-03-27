-- part of a query repo
-- query name: Volume Market Share
-- query link: https://dune.com/queries/95179


SELECT
    date_trunc('week', block_time) AS week,
    CASE 
        WHEN project = 'uniswap' AND version = '3' THEN 'Uniswap V3' 
        WHEN project = 'uniswap' AND version != '3' THEN 'Uniswap V1+V2' 
        ELSE project END AS project,
    SUM(amount_usd) AS usd_volume,
    COUNT(*) AS n_trades
FROM dex.trades t                                                                             
WHERE block_time >= TIMESTAMP '{{2. Start date}}'
AND block_time <= TIMESTAMP '{{3. End date}}'
AND project IN ('balancer', 'curve', 'uniswap', 'sushiswap', 'Bancor Network')
AND ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
GROUP BY 1, 2
ORDER BY 3, 2