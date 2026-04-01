-- part of a query repo
-- query name: TVL (Pools Overview)
-- query link: https://dune.com/queries/2872975


WITH
    v1_tvl AS (
        SELECT 
            'ethereum_' AS blockchain, 
            SUM(usd_amount) AS tvl 
        FROM (
            SELECT SUM(protocol_liquidity_usd) as usd_amount FROM balancer_v1_ethereum.liquidity
            WHERE day = (CURRENT_DATE - interval '1' day)
                                AND protocol_liquidity_usd < 1000000000
        )
    )
    , v2_tvl AS (
        SELECT 
            blockchain, 
            SUM(protocol_liquidity_usd) AS tvl 
        FROM balancer.liquidity 
        WHERE day = (CURRENT_DATE - interval '1' day)
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
            a.tvl, 
            t.total_tvl,
            a.tvl / t.total_tvl AS percentage_tvl,
            t.total_tvl/1e9 as short_tvl
        FROM all_tvl a
        LEFT JOIN (SELECT 1 AS rn, SUM(tvl) AS total_tvl FROM all_tvl) t ON a.rn = t.rn
        WHERE ('{{4. Blockchain}}' = 'All' OR a.blockchain = '{{4. Blockchain}}')
    )
SELECT * FROM tvl