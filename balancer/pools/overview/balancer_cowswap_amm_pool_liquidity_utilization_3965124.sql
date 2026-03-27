-- part of a query repo
-- query name: Balancer CoWSwap AMM Pool Liquidity Utilization
-- query link: https://dune.com/queries/3965124


WITH 
    swaps AS (
        SELECT
            date_trunc('month', d.block_time) AS day,
            SUM(amount_usd) AS volume
        FROM balancer_cowswap_amm.trades d
        WHERE blockchain = '{{4. Blockchain}}'
        AND ('{{1. Pool Address}}' = 'All' OR project_contract_address = {{1. Pool Address}})
        GROUP BY 1
    ),

    total_tvl AS (
        SELECT date_trunc('month', day) AS day, SUM(protocol_liquidity_usd) AS tvl
        FROM balancer_cowswap_amm.liquidity
        WHERE ('{{1. Pool Address}}' = 'All' OR
        pool_id = {{1. Pool Address}})
        AND blockchain = '{{4. Blockchain}}'
        GROUP BY 1
    )
   
SELECT
    CAST(t.day as timestamp) as day,
    (s.volume)/(t.tvl) AS Ratio,
    s.volume,
    t.tvl
FROM total_tvl t
LEFT JOIN swaps s ON s.day = t.day
WHERE t.day >= TIMESTAMP '{{2. Start date}}'
AND t.day <= TIMESTAMP '{{3. End date}}'
ORDER BY 1