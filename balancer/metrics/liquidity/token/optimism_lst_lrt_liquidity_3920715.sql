-- part of a query repo
-- query name: Optimism LST/LRT Liquidity
-- query link: https://dune.com/queries/3920715


SELECT 
    CAST(day AS TIMESTAMP) AS day, 
    l.symbol, 
    SUM(protocol_liquidity_eth) AS liquidity  
FROM balancer_v2_optimism.liquidity t
INNER JOIN dune.balancer.result_lst_tokens l 
ON t.token_address = l.contract_address
AND l.blockchain = t.blockchain
WHERE t.day >= TIMESTAMP '{{Start date}}'
AND t.day <= TIMESTAMP '{{End date}}'
AND t.blockchain = 'optimism'
GROUP BY 1,2
HAVING SUM(protocol_liquidity_usd) > 1000
ORDER BY 1 DESC, 3 DESC
