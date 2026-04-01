-- part of a query repo
-- query name: Balancer Median Weekly Liquidity by Blockchain
-- query link: https://dune.com/queries/3547951


WITH

liquidity AS (
    SELECT 
        day,
        blockchain,
        CASE WHEN '{{4. Currency}}' = 'USD'
        THEN SUM(pool_liquidity_usd) 
        WHEN '{{4. Currency}}' = 'ETH'
        THEN SUM(pool_liquidity_eth) 
        END AS tvl
    FROM balancer.liquidity t
    WHERE day >= TIMESTAMP '{{1. Start date}}' 
    AND ('{{3. Blockchain}}' = 'All' OR t.blockchain = '{{3. Blockchain}}')
    AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
    AND protocol_liquidity_usd < 1000000000
    GROUP BY 1, 2
)

SELECT
    CAST(DATE_TRUNC('week', day) AS timestamp) AS week,
         s.blockchain || 
    CASE 
        WHEN s.blockchain = 'arbitrum' THEN ' 🟦'
        WHEN s.blockchain = 'avalanche_c' THEN ' ⬜ '
        WHEN s.blockchain = 'base' THEN ' 🟨'
        WHEN s.blockchain = 'ethereum' THEN ' Ξ'
        WHEN s.blockchain = 'gnosis' THEN ' 🟩'
        WHEN s.blockchain = 'optimism' THEN ' 🔴'
        WHEN s.blockchain = 'polygon' THEN ' 🟪'
        WHEN s.blockchain = 'zkevm' THEN ' 🟣'
    END 
AS blockchain,
    APPROX_PERCENTILE(tvl, 0.5) AS median_liquidity
FROM liquidity s
GROUP BY 1, 2
