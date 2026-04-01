-- part of a query repo
-- query name: Optimism LST/LRT Liquidity Growth
-- query link: https://dune.com/queries/3939196


WITH initial_tvl AS (
    SELECT 
        l.symbol, 
        SUM(protocol_liquidity_usd) AS initial_liquidity  
    FROM dune.balancer.result_lst_pools_liquidity t
    INNER JOIN dune.balancer.result_lst_tokens l 
    ON t.token_address = l.contract_address
    AND l.blockchain = t.blockchain
    WHERE t.day = TIMESTAMP '{{Start date}}'
    AND t.blockchain = 'optimism'
    GROUP BY 1
    ORDER BY 1 DESC, 2 DESC
)

SELECT 
    CAST(day AS TIMESTAMP) AS day, 
    l.symbol, 
    SUM(protocol_liquidity_usd / i.initial_liquidity)  AS liquidity_growth  
FROM dune.balancer.result_lst_pools_liquidity t
INNER JOIN dune.balancer.result_lst_tokens l 
ON t.token_address = l.contract_address
AND l.blockchain = t.blockchain
LEFT JOIN initial_tvl i 
ON i.symbol = l.symbol
WHERE t.day >= TIMESTAMP '{{Start date}}'
AND t.day <= TIMESTAMP '{{End date}}'
AND t.blockchain = 'optimism'
GROUP BY 1,2
HAVING SUM(protocol_liquidity_usd) > 1000
ORDER BY 1 DESC, 3 DESC
