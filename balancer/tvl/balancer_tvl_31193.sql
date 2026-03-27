-- part of a query repo
-- query name: Balancer TVL
-- query link: https://dune.com/queries/31193


WITH 
    all_tvl AS (
        SELECT 
            1 AS rn,
            blockchain, 
            SUM(protocol_liquidity_usd) AS tvl 
        FROM balancer.liquidity 
        WHERE day = date_trunc('day', NOW()) - INTERVAL '1' DAY 
        AND ('{{5. Version}}' = 'All' OR '{{5. Version}}' = version)
        GROUP BY 1, 2
    )
    
    , tvl AS (
        SELECT 
          a.blockchain || 
            CASE 
                WHEN a.blockchain = 'arbitrum' THEN ' 🟦 |'
                WHEN a.blockchain = 'avalanche_c' THEN ' ⬜  |'
                WHEN a.blockchain = 'base' THEN ' 🟨 |'
                WHEN a.blockchain = 'ethereum' THEN ' Ξ |'
                WHEN a.blockchain = 'gnosis' THEN ' 🟩 |'
                WHEN a.blockchain = 'optimism' THEN ' 🔴 |'
                WHEN a.blockchain = 'polygon' THEN ' 🟪 |'
                WHEN a.blockchain = 'zkevm' THEN ' 🟣 |'
            END 
        AS blockchain,            
        a.tvl, 
            t.total_tvl,
            a.tvl / t.total_tvl AS percentage_tvl,
            t.total_tvl/1e6 as short_tvl
        FROM all_tvl a
        LEFT JOIN (SELECT 1 AS rn, SUM(tvl) AS total_tvl FROM all_tvl) t ON a.rn = t.rn
        WHERE ('{{4. Blockchain}}' = 'All' OR a.blockchain = '{{4. Blockchain}}')
    )
SELECT * FROM tvl