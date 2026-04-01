-- part of a query repo
-- query name: LSTs Fees Collected Variation
-- query link: https://dune.com/queries/3468667


WITH lst_pools AS (
    SELECT * FROM dune.balancer.result_lst_pools
),
fees AS (
    SELECT 
    CASE WHEN 
        '{{5. Aggregation}}' = 'Monthly'
    THEN CAST(DATE_TRUNC('month', t.day) AS TIMESTAMP) 
    WHEN 
        '{{5. Aggregation}}' = 'Weekly'
    THEN CAST(DATE_TRUNC('week', t.day) AS TIMESTAMP) 
        WHEN 
        '{{5. Aggregation}}' = 'Daily'
    THEN CAST(DATE_TRUNC('day', t.day) AS TIMESTAMP) 
    END AS date, 
        CASE WHEN '{{4. Currency}}' = 'USD' THEN SUM(protocol_fee_collected_usd)
             WHEN '{{4. Currency}}' = 'eth' THEN SUM(protocol_fee_collected_usd / median_price_eth)
        END AS amount_usd
    FROM balancer.protocol_fee t
    INNER JOIN lst_pools l ON l.pool_address = t.pool_address
                            AND l.blockchain = t.blockchain
    LEFT JOIN dune.balancer.result_eth_price p ON t.day = p.day
    WHERE t.day >= TIMESTAMP '{{1. Start date}}'
    AND t.day <= TIMESTAMP '{{2. End date}}'
    AND ('{{3. Blockchain}}' = 'All' OR t.blockchain = '{{3. Blockchain}}')
    GROUP BY 1
)

SELECT 
    CAST(date AS TIMESTAMP) AS date,
    amount_usd,
    (amount_usd - LAG(amount_usd) OVER (ORDER BY date)) / LAG(amount_usd) OVER (ORDER BY date) AS delta_amount_usd
FROM fees