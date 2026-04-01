-- part of a query repo
-- query name: LSTs Median Liquidity Variation
-- query link: https://dune.com/queries/3468675


WITH lst_pools AS (
    SELECT * FROM dune.balancer.result_lst_pools
),
fees AS (
    SELECT 
        CASE 
            WHEN '{{5. Aggregation}}' = 'Monthly' THEN CAST(DATE_TRUNC('month', t.day) AS TIMESTAMP) 
            WHEN '{{5. Aggregation}}' = 'Weekly' THEN CAST(DATE_TRUNC('week', t.day) AS TIMESTAMP) 
            WHEN '{{5. Aggregation}}' = 'Daily' THEN CAST(DATE_TRUNC('day', t.day) AS TIMESTAMP) 
        END AS date, 
        CASE 
            WHEN '{{4. Currency}}' = 'USD' THEN SUM(protocol_liquidity_usd)
            WHEN '{{4. Currency}}' = 'eth' THEN SUM(protocol_liquidity_eth)
        END AS amount_usd
    FROM balancer.liquidity t
    INNER JOIN lst_pools l ON l.pool_address = t.pool_address
                            AND l.blockchain = t.blockchain
    WHERE t.day >= TIMESTAMP '{{1. Start date}}'
    AND t.day <= TIMESTAMP '{{2. End date}}'
    AND ('{{3. Blockchain}}' = 'All' OR t.blockchain = '{{3. Blockchain}}')
    GROUP BY 1
),
delta AS (
    SELECT 
        date AS date,
        APPROX_PERCENTILE(amount_usd, 0.5) AS median_liquidity
    FROM fees
    GROUP BY 1
)

SELECT 
    date,
    median_liquidity,
    (median_liquidity - LAG(median_liquidity) OVER (ORDER BY date)) / LAG(median_liquidity) OVER (ORDER BY date) AS delta_amount_usd
FROM delta
GROUP BY 1, 2;