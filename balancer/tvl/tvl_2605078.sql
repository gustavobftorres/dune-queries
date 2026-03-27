-- part of a query repo
-- query name: TVL
-- query link: https://dune.com/queries/2605078


WITH 
    v1_tvl AS (
        SELECT 
            'ethereum (v1)' as blockchain, 
            SUM(protocol_liquidity_usd) AS tvl 
        FROM balancer.liquidity 
        WHERE day = date_trunc('day', NOW()) - INTERVAL '1' DAY
        AND version = '1'
        AND protocol_liquidity_usd < 1000000000
        GROUP BY 1)
    
    , v2_tvl AS (
        SELECT 
            blockchain, 
            SUM(protocol_liquidity_usd) AS tvl 
        FROM balancer.liquidity 
        WHERE day = date_trunc('day', NOW()) - INTERVAL '1' DAY
        AND version = '2'
        GROUP BY 1
    ),

    all_tvl AS ( 
        SELECT 1 AS rn, * FROM v1_tvl 
        UNION ALL
        SELECT 1 AS rn, * FROM v2_tvl
    )
    , tvl AS (
        SELECT 
            a.blockchain, 
            SUM(a.tvl) as tvl, 
            t.total_tvl,
            a.tvl / t.total_tvl AS percentage_tvl,
            CASE WHEN '{{3. Blockchain}}' = 'All' THEN t.total_tvl/1e9 
            ELSE SUM(a.tvl)/1e9
            END as short_tvl
        FROM all_tvl a
        LEFT JOIN (SELECT 1 AS rn, SUM(tvl) AS total_tvl FROM all_tvl) t ON a.rn = t.rn
        WHERE ('{{3. Blockchain}}' = 'All' OR a.blockchain = '{{3. Blockchain}}')
        GROUP BY 1,3,4
    )
SELECT * FROM tvl