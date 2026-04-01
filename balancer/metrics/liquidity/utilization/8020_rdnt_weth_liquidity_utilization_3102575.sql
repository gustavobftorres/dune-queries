-- part of a query repo
-- query name: 8020 RDNT WETH Liquidity Utilization
-- query link: https://dune.com/queries/3102575


WITH 
    swaps AS (
        SELECT
            date_trunc('day', d.block_time) AS day,
            SUM(amount_usd) AS volume
        FROM dex.trades d
        WHERE project = 'balancer' AND version = '2' AND blockchain = 'arbitrum'
        AND project_contract_address = 0x32df62dc3aed2cd6224193052ce665dc18165841
        GROUP BY 1
    ),

    total_tvl AS (
        SELECT date_trunc('day', day) AS day, SUM(pool_liquidity_usd) AS tvl
        FROM balancer.liquidity
        WHERE BYTEARRAY_SUBSTRING(pool_id,1,20) = 0x32df62dc3aed2cd6224193052ce665dc18165841
        AND blockchain = 'arbitrum'
        GROUP BY 1
    )
   
SELECT
    CAST(t.day as timestamp) as day,
    (s.volume)/(t.tvl) AS Ratio,
    s.volume,
    t.tvl
FROM total_tvl t
LEFT JOIN swaps s ON s.day = t.day
ORDER BY 1