-- part of a query repo
-- query name: Balancer TVL by Pool on Mainnet
-- query link: https://dune.com/queries/31278


WITH labels AS (
        SELECT * FROM (SELECT
            address,
            name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.labels
        WHERE "type" IN ('balancer_pool', 'balancer_v2_pool')
        GROUP BY 1, 2) l
        WHERE num = 1
        
        UNION ALL
        
        SELECT 
            '\x32296969ef14eb0c6d29669c550d4a0449130230' AS address, 
            'B-stETH-STABLE' AS name,
            1 AS num
    ),

    tvl AS (
        SELECT p.day,  pool, SUM(p.liquidity) AS tvl
        FROM balancer."view_pools_liquidity" p
        WHERE pool <> '\xBA12222222228d8Ba445958a75a0704d566BF2C8'
        AND pool NOT IN ('\xa5da8cc7167070b62fdcb332ef097a55a68d8824', '\x72cd8f4504941bf8c5a21d1fd83a96499fd71d2c', '\xe036cce08cf4e23d33bc6b18e53caf532afa8513', '\x02ec2c01880a0673c76e12ebe6ff3aad0a8da968')
        AND day <= '{{3. End date}}'
        AND pool NOT IN (
            '\x9210f1204b5a24742eba12f710636d76240df3d00000000000000000000000fc',
            '\x2bbf681cc4eb09218bee85ea2a5d3d13fa40fc0c0000000000000000000000fd',
            '\x804cdb9116a10bb78768d3252355a1b18067bf8f0000000000000000000000fb'
        )
        GROUP BY 1, 2
    ),
    
    total_tvl AS (
        SELECT day, 'Total' AS pool, SUM(tvl) AS tvl
        FROM tvl
        GROUP BY 1, 2
    ),
    
    top_pools AS (
        SELECT DISTINCT pool, tvl, CONCAT(SUBSTRING(UPPER(l.name), 0, 15), ' (', SUBSTRING(t.pool::text, 3, 8), ')') AS symbol
        FROM tvl t
        LEFT JOIN labels l ON l.address = SUBSTRING(t.pool::text, 0, 43)::bytea
        WHERE day = LEAST(CURRENT_DATE, '{{3. End date}}')
        AND tvl IS NOT NULL
        ORDER BY 2 DESC, 3 DESC 
        LIMIT 7
    )

SELECT * FROM total_tvl
WHERE day >= '{{2. Start date}}'

UNION ALL

SELECT t.day, COALESCE(p.symbol, 'Others') AS pool, SUM(t.tvl) AS "TVL"
FROM tvl t
LEFT JOIN top_pools p ON p.pool = t.pool
WHERE day >= '{{2. Start date}}'
GROUP BY 1, 2
ORDER BY 1