-- part of a query repo
-- query name: LST / LRT Liquidity Difference (USD)
-- query link: https://dune.com/queries/4049431


WITH tvl AS(
SELECT 
    CAST(day AS TIMESTAMP) AS day, 
    l.symbol, 
    SUM(protocol_liquidity_usd) AS liquidity  
FROM balancer_v2_optimism.liquidity t
INNER JOIN dune.balancer.result_lst_tokens l 
ON t.token_address = l.contract_address
AND l.blockchain = t.blockchain
WHERE t.day >= TIMESTAMP '{{Start date}}'
AND t.blockchain = 'optimism'
GROUP BY 1,2
ORDER BY 1 DESC, 3 DESC
),

tvl_2 AS(
SELECT
    SUM(CASE WHEN day = (SELECT MAX(day) FROM tvl) THEN liquidity ELSE 0 END) AS current_liquidity,
    SUM(CASE WHEN day = (SELECT MIN(day) FROM tvl) THEN liquidity ELSE 0 END) AS initial_liquidity
FROM tvl
)

SELECT 
    (SUM(current_liquidity / initial_liquidity) - 1) * 100  AS delta
FROM tvl_2