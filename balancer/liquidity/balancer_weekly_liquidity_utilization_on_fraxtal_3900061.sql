-- part of a query repo
-- query name: Balancer Weekly Liquidity Utilization on Fraxtal
-- query link: https://dune.com/queries/3900061


WITH 
    swaps AS (
        SELECT
            date_trunc('week', CAST(day AS TIMESTAMP)) AS day,
            SUM(amount_usd) AS volume
        FROM dune.balancer.dataset_fraxtal_snapshots d
        GROUP BY 1
    ),

    total_tvl AS (
        SELECT date_trunc('week', CAST(day AS TIMESTAMP)) AS day, SUM(CAST(protocol_liquidity_usd AS double)) AS tvl
        FROM dune.balancer.dataset_fraxtal_snapshots
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
AND t.day <= TIMESTAMP '{{2. End date}}'
ORDER BY 1
