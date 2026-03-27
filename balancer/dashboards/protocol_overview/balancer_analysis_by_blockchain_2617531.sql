-- part of a query repo
-- query name: Balancer Analysis by Blockchain
-- query link: https://dune.com/queries/2617531


WITH swaps AS
    (SELECT
        blockchain,
        SUM(amount_usd) AS total_amount_usd,
        SUM(CASE WHEN project = 'balancer' THEN amount_usd ELSE 0 END) AS balancer_amount_usd,
        CASE WHEN SUM(amount_usd) > 0 THEN SUM(CASE WHEN project = 'balancer' THEN amount_usd ELSE 0 END) / SUM(amount_usd)  ELSE 0 END AS market_share
    FROM dex.trades
    WHERE blockchain IN ('polygon', 'ethereum', 'arbitrum', 'gnosis', 'optimism','base','avalanche_c', 'zkevm') AND block_time >= CAST(NOW() AS TIMESTAMP) - INTERVAL '1' DAY
    GROUP BY 1),
    
    fees AS (
        SELECT 
            AVG(swap_fee_percentage/1e18) AS fee, 
            blockchain
        FROM balancer.pools_fees
        GROUP BY blockchain),
        
    revenue AS(
        SELECT 
            blockchain,
            SUM(protocol_fee_collected_usd) as revenue
        FROM balancer.protocol_fee
        WHERE day > now() - interval '30' day
        GROUP BY 1
    )
        
SELECT 
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
    total_amount_usd, 
    balancer_amount_usd, 
    market_share, 
    t.tvl, 
    t.percentage_tvl, 
    revenue as revenue_30d,
    avg(fee) as fee
from swaps s
LEFT JOIN fees f on s.blockchain = f.blockchain
LEFT JOIN query_2655650 t on s.blockchain = t.blockchain
LEFT JOIN revenue p ON p.blockchain = s.blockchain
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 3 DESC
