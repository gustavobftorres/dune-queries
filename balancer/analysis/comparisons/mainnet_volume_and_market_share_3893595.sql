-- part of a query repo
-- query name: Mainnet Volume and Market Share
-- query link: https://dune.com/queries/3893595


WITH mainnet_swaps AS (
    SELECT 
        block_date,
        sum(amount_usd) AS volume
    FROM dex.trades
    WHERE blockchain = 'ethereum'
    AND block_date > now() - interval '30' day
    GROUP BY 1
),

balancer_swaps AS (
    SELECT 
        block_date,
        sum(amount_usd) AS b_volume
    FROM dex.trades
    WHERE blockchain = 'ethereum'
    AND project = 'balancer'
    AND block_date > now() - interval '30' day
    GROUP BY 1
)

SELECT 
    b.block_date,
    b.b_volume AS balancer_volume,
    b.b_volume / m.volume AS balancer_mkt_share
FROM balancer_swaps b
JOIN mainnet_swaps m ON b.block_date = m.block_date
ORDER BY 1 DESC
    
    