-- part of a query repo
-- query name: LSTs Volume by Token Pair - Table
-- query link: https://dune.com/queries/3372577


WITH lst_pools AS (
    SELECT * FROM dune.balancer.result_lst_pools
),
trades AS (
    SELECT 
        ROW_NUMBER() OVER(ORDER BY SUM(amount_usd) DESC) AS rn, 
        t.blockchain, 
        token_pair, 
        CASE WHEN 
        '{{4. Currency}}' = 'USD'
        THEN SUM(amount_usd)
        WHEN
        '{{4. Currency}}' = 'eth'
        THEN SUM(amount_usd / median_price_eth)
        END AS volume,
        CASE WHEN 
        '{{4. Currency}}' = 'USD'
        THEN SUM(t.amount_usd) FILTER (WHERE t.block_time >= NOW() - INTERVAL '7' DAY)
        WHEN
        '{{4. Currency}}' = 'eth'
        THEN  SUM(t.amount_usd / median_price_eth) FILTER(WHERE t.block_time >= NOW() - INTERVAL '7' DAY)
        END AS volume_30d
    FROM balancer.trades t
    INNER JOIN lst_pools l ON l.pool_address = t.project_contract_address
                            AND l.blockchain = t.blockchain
    LEFT JOIN dune.balancer.result_eth_price p ON t.block_date = p.day
    WHERE t.block_date >= TIMESTAMP '{{1. Start date}}'
    AND t.block_date <= TIMESTAMP '{{2. End date}}'
    AND ('{{3. Blockchain}}' = 'All' OR t.blockchain = '{{3. Blockchain}}')
    GROUP BY 2,3
)

SELECT * FROM trades
ORDER BY 4 DESC;
