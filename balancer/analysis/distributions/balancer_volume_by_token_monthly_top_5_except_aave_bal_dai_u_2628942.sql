-- part of a query repo
-- query name: Balancer Volume by Token (monthly top 5 except AAVE, BAL, DAI, USDC, WBTC & WETH)
-- query link: https://dune.com/queries/2628942


WITH swaps AS (
        SELECT
            date_trunc('month', d.block_time) AS month,
            sum(amount_usd) AS volume,
            CAST (d.token_bought_address as varchar) AS address,
            t.symbol AS token,
            d.blockchain
        FROM dex.trades d
        LEFT JOIN tokens.erc20 t ON CAST(t.contract_address as varchar) = CAST(d.token_bought_address as varchar)
        AND t.blockchain = d.blockchain
        WHERE project = 'balancer' AND (('{{4. Blockchain}}' = 'All' AND 1 = 1) OR d.blockchain = '{{4. Blockchain}}')
        AND t.symbol NOT IN ('WETH', 'AAVE', 'BAL', 'DAI', 'USDC', 'WBTC')
        GROUP BY 1,3,4,5
        
        UNION ALL 
        
        SELECT
            date_trunc('month', d.block_time) AS month,
            sum(amount_usd) AS volume,
            CAST (d.token_sold_address as varchar) AS address,
            t.symbol AS token,
            d.blockchain
        FROM dex.trades d
        LEFT JOIN tokens.erc20 t ON CAST (t.contract_address as varchar) = CAST(d.token_sold_address as varchar)
        WHERE project = 'balancer' AND (('{{4. Blockchain}}' = 'All' AND 1 = 1) OR d.blockchain = '{{4. Blockchain}}')
        AND t.symbol NOT IN ('WETH', 'AAVE', 'BAL', 'DAI', 'USDC', 'WBTC')
        GROUP BY 1,3,4,5
)

SELECT * FROM (
        SELECT
        s.month,
        CONCAT( CASE 
            WHEN s.blockchain = 'arbitrum' THEN ' 🟦 |'
            WHEN s.blockchain = 'avalanche_c' THEN ' ⬜  |'
            WHEN s.blockchain = 'base' THEN ' 🟨 |'
            WHEN s.blockchain = 'ethereum' THEN ' Ξ |'
            WHEN s.blockchain = 'gnosis' THEN ' 🟩 |'
            WHEN s.blockchain = 'optimism' THEN ' 🔴 |'
            WHEN s.blockchain = 'polygon' THEN ' 🟪 |'
            WHEN s.blockchain = 'zkevm' THEN ' 🟣 |'
            END 
            , ' ', COALESCE(s.token, CONCAT(SUBSTRING(s.address, 3, 6), '...'))) AS token,
        s.address,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY SUM(volume) DESC NULLS LAST) AS position,
        sum(s.volume)/2 AS volume
    FROM swaps s
    GROUP BY 1, 2, 3
    ORDER BY 1, 3
) ranking
WHERE position <= 5