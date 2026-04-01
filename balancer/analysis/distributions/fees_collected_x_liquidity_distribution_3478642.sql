-- part of a query repo
-- query name: Fees Collected x Liquidity Distribution
-- query link: https://dune.com/queries/3478642


WITH 
    fees AS(
    SELECT 
        f.blockchain,
        pool_id,
        pool_symbol,
        sum(protocol_fee_collected_usd) AS fees
    FROM balancer.protocol_fee f
    LEFT JOIN dune.balancer.dataset_core_pools c 
    ON c.network = f.blockchain AND c.pool = f.pool_id
    WHERE day >= now() - interval '{{Last x Days}}' day
    AND ('{{Pool Address}}' = 'All' OR CAST(pool_address AS VARCHAR) = '{{Pool Address}}')
    AND ('{{Blockchain}}' = 'All' OR f.blockchain = '{{Blockchain}}')
    AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
    AND ('{{Pool Type}}' = 'All' OR f.pool_type = '{{Pool Type}}')
    GROUP BY 1, 2, 3
    ORDER BY 4 DESC
),

tvl AS(
    SELECT 
        blockchain,
        pool_id,
        sum(protocol_liquidity_usd) AS tvl
    FROM balancer.liquidity
    WHERE day = (SELECT max(day) FROM balancer.liquidity WHERE version = '2')
    GROUP BY 1,2
    HAVING SUM(protocol_liquidity_usd) > 1000    
),

swaps AS(
    SELECT 
        blockchain,
        pool_id,
        sum(amount_usd) AS volume
    FROM balancer.trades
    WHERE block_date >= now() - interval '{{Last x Days}}' day
    GROUP BY 1,2
)

SELECT *
    FROM(
    SELECT
    f.blockchain,
    f.pool_id,
    CONCAT(CASE 
            WHEN f.blockchain = 'arbitrum' THEN ' 🟦 |'
            WHEN f.blockchain = 'avalanche_c' THEN ' ⬜  |'
            WHEN f.blockchain = 'base' THEN ' 🟨 |'
            WHEN f.blockchain = 'ethereum' THEN ' Ξ |'
            WHEN f.blockchain = 'gnosis' THEN ' 🟩 |'
            WHEN f.blockchain = 'optimism' THEN ' 🔴 |'
            WHEN f.blockchain = 'polygon' THEN ' 🟪 |'
            WHEN f.blockchain = 'zkevm' THEN ' 🟣 |'
        END 
        , ' ', f.pool_symbol)
         AS pool_symbol,
    ROUND(SUM(f.fees), 2) AS "Fees Collected (USD)",
    ROUND(SUM(t.tvl), 2) AS "TVL (USD)",
    ROUND(SUM(s.volume), 2) AS "Volume (USD)",
    ROW_NUMBER() OVER(ORDER BY SUM(fees) DESC) AS rn
FROM fees f
INNER JOIN tvl t 
ON f.blockchain = t.blockchain
AND f.pool_id = t.pool_id
LEFT JOIN swaps s 
ON f.blockchain = s.blockchain
AND f.pool_id = s.pool_id
GROUP BY 1, 2, 3
)
    WHERE rn <= {{Top x Pools}}