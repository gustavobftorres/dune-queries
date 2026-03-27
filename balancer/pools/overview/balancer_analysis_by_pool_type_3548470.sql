-- part of a query repo
-- query name: Balancer Analysis by Pool Type
-- query link: https://dune.com/queries/3548470


WITH 
    swaps AS(
        SELECT
            t.pool_type,
            version,
            sum(amount_usd) AS volume
        FROM balancer.trades t
        WHERE block_date >= TIMESTAMP '{{1. Start date}}'
        AND block_date <= TIMESTAMP '{{2. End date}}'
        AND ('{{3. Blockchain}}' = 'All' OR t.blockchain = '{{3. Blockchain}}')
        GROUP BY 1, 2),
        
    fees AS(
        SELECT 
            pool_type,
            SUM(protocol_fee_collected_usd) as fees_collected
        FROM balancer.protocol_fee f
        WHERE day >= TIMESTAMP '{{1. Start date}}'
        AND day <= TIMESTAMP '{{2. End date}}'
        AND ('{{3. Blockchain}}' = 'All' OR f.blockchain = '{{3. Blockchain}}')
        GROUP BY 1
    ),
    
    liquidity AS(
        SELECT
            CASE WHEN pool_type = 'balancer_cowswap_amm'
            THEN 'Balancer CoWSwap AMM'
            WHEN pool_type = 'V1'
            THEN 'v1'
            ELSE pool_type
            END AS pool_type,
            sum(protocol_liquidity_usd) AS tvl
        FROM balancer.liquidity p
        WHERE day = (SELECT max(day) FROM balancer.liquidity)
        AND ('{{3. Blockchain}}' = 'All' OR p.blockchain = '{{3. Blockchain}}')
        GROUP BY 1
)
        
SELECT 
    s.pool_type, 
    version,
    volume, 
    volume / (SELECT sum(volume) FROM swaps) AS volume_share,
    t.tvl, 
    tvl / (SELECT sum(tvl) FROM liquidity) AS tvl_share, 
    fees_collected as fees_30d,
    fees_collected / (SELECT sum(fees_collected) FROM fees) AS fees_share
from swaps s
LEFT JOIN fees f on s.pool_type = f.pool_type
LEFT JOIN liquidity t on s.pool_type = t.pool_type
WHERE ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
AND s.pool_type IS NOT NULL
ORDER BY 3 DESC
