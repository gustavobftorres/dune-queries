-- part of a query repo
-- query name: Balancer TVL
-- query link: https://dune.com/queries/2617560


WITH tvl_v1 AS (
    SELECT 'V1' as version, CAST(day as timestamp) as day, SUM(usd_amount) AS tvl
    FROM balancer_v1_ethereum.liquidity
    WHERE pool_id NOT IN (
        '0x454c1d458f9082252750ba42d60fae0887868a3b',
        '0x6b9887422e2a4ae11577f59ea9c01a6c998752e2',
        '0x666f55ada362039dfedfca9e8e2db2f874d1dfcd',
        '0xa5da8cc7167070b62fdcb332ef097a55a68d8824',
        '0x72cd8f4504941bf8c5a21d1fd83a96499fd71d2c',
        '0xe036cce08cf4e23d33bc6b18e53caf532afa8513',
        '0x02ec2c01880a0673c76e12ebe6ff3aad0a8da968'
    )
    GROUP BY 1, 2
),

tvl_eth AS (
    SELECT 'V2' as version, CAST(day as timestamp) as day, SUM(protocol_liquidity_usd) AS tvl
    FROM balancer_v2_ethereum.liquidity
    GROUP BY 1, 2
),

tvl_arb AS (
    SELECT 'V2' as version, CAST(day as timestamp) as day, SUM(protocol_liquidity_usd) AS tvl
    FROM balancer_v2_arbitrum.liquidity
    GROUP BY 1, 2
),

tvl_opt AS (
    SELECT 'V2' as version, CAST(day as timestamp) as day, SUM(protocol_liquidity_usd) AS tvl
    FROM balancer_v2_optimism.liquidity
    GROUP BY 1, 2
),

tvl_pol AS (
    SELECT 'V2' as version, CAST(day as timestamp) as day, SUM(protocol_liquidity_usd) AS tvl
    FROM balancer_v2_polygon.liquidity
    GROUP BY 1, 2
),

tvl_gno AS (
    SELECT 'V2' as version, CAST(day as timestamp) as day, SUM(protocol_liquidity_usd) AS tvl
    FROM balancer_v2_gnosis.liquidity
    GROUP BY 1, 2
)

SELECT v1.day, v1.tvl AS "V1", v2.tvl AS "V2", COALESCE(v1.tvl, 0) + COALESCE(v2.tvl, 0) + COALESCE(v3.tvl, 0) + COALESCE(v4.tvl, 0) + 
COALESCE(v5.tvl, 0) + COALESCE(v6.tvl, 0) AS "Total"
FROM tvl_v1 v1
FULL OUTER JOIN tvl_eth v2 ON v1.day = v2.day
FULL OUTER JOIN tvl_arb v3 ON v1.day = v3.day
FULL OUTER JOIN tvl_opt v4 ON v1.day = v4.day
FULL OUTER JOIN tvl_pol v5 ON v1.day = v5.day
FULL OUTER JOIN tvl_gno v6 ON v1.day = v5.day
WHERE v1.tvl < 5e9 AND v1.day NOT IN (TIMESTAMP '2023-02-06 00:00')