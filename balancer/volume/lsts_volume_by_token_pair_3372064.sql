-- part of a query repo
-- query name: LSTs Volume by Token Pair
-- query link: https://dune.com/queries/3372064


WITH lst_pools AS(
SELECT * FROM dune.balancer.result_lst_pools),

trades AS (
SELECT 
        CASE WHEN 
        '{{5. Aggregation}}' = 'Monthly'
    THEN CAST(block_month AS TIMESTAMP) 
    WHEN 
        '{{5. Aggregation}}' = 'Weekly'
    THEN CAST(DATE_TRUNC('week', block_date) AS TIMESTAMP) 
        WHEN 
        '{{5. Aggregation}}' = 'Daily'
    THEN CAST(DATE_TRUNC('day', block_date) AS TIMESTAMP) 
    END AS date, 
        token_pair, 
        CASE WHEN 
        '{{4. Currency}}' = 'USD'
        THEN SUM(amount_usd)
        WHEN
        '{{4. Currency}}' = 'eth'
        THEN SUM(amount_usd / median_price_eth)
        END AS amount_usd
FROM balancer.trades t
INNER JOIN lst_pools l ON l.pool_address = t.project_contract_address
                        AND l.blockchain = t.blockchain
LEFT JOIN dune.balancer.result_eth_price p ON t.block_date = p.day
WHERE t.block_date >= TIMESTAMP '{{1. Start date}}'
AND t.block_date <= TIMESTAMP '{{2. End date}}'
AND ('{{3. Blockchain}}' = 'All' OR t.blockchain = '{{3. Blockchain}}')
GROUP BY 1, 2
HAVING sum(amount_usd) > 1000
)

SELECT * FROM trades