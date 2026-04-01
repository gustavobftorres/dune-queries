-- part of a query repo
-- query name: Balancer CoWSwap AMM Weekly Liquidity Utilization
-- query link: https://dune.com/queries/3954600


WITH 
    swaps AS (
        SELECT
            date_trunc('week', d.block_time) AS day,
            SUM(amount_usd) AS volume
        FROM balancer_cowswap_amm.trades d
        WHERE ('{{2. Blockchain}}' = 'All' OR blockchain = '{{2. Blockchain}}')
        GROUP BY 1
    ),

    total_tvl AS (
        SELECT date_trunc('week', day) AS day, SUM(protocol_liquidity_usd) AS tvl
        FROM balancer_cowswap_amm.liquidity
        WHERE ('{{2. Blockchain}}' = 'All' OR blockchain = '{{2. Blockchain}}')
        AND day <= (SELECT MAX(day) FROM balancer.liquidity WHERE version = '2')
        GROUP BY 1
    )
   
SELECT
    CAST(t.day as timestamp) as day,
    (s.volume)/(t.tvl) AS Ratio,
    s.volume,
    t.tvl
FROM total_tvl t
LEFT JOIN swaps s ON s.day = t.day
WHERE t.day >= TIMESTAMP '{{1. Start date}}'
ORDER BY 1
