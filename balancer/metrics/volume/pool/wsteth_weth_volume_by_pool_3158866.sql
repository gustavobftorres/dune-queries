-- part of a query repo
-- query name: wstETH/WETH volume by pool
-- query link: https://dune.com/queries/3158866


WITH labels AS (
SELECT address, blockchain, name
FROM labels.balancer_v2_pools
UNION ALL
SELECT 0xf01b0684c98cd7ada480bfdf6e43876422fa1fc1 as address, 'ethereum' as blockchain, 'GYRO ECLP wstETH/WETH'
UNION ALL
SELECT 0xf7a826d47c8e02835d94fb0aa40f0cc9505cb134 as address, 'ethereum' as blockchain, 'GYRO ECLP wstETH/cbETH'
)


SELECT t.pool_id, l.name, sum(t.amount_usd) as volume 
FROM balancer.trades t
LEFT JOIN query_3150087 q ON t.tx_hash = q.hash 
LEFT JOIN labels l ON BYTEARRAY_SUBSTRING(t.pool_id, 1, 20) = l.address AND t.blockchain = l.blockchain 
WHERE q.liquidity_src = 'Balancer' AND q.token_pair = 'WETH-wstETH' AND t.blockchain = 'ethereum'
GROUP BY 1,2
ORDER BY 3 DESC
