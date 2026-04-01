-- part of a query repo
-- query name: LST Liquidity on Balancer
-- query link: https://dune.com/queries/3372735


SELECT 
    CAST(day AS TIMESTAMP) AS day, 
    l.symbol, 
    CASE WHEN '{{4. Currency}}' = 'USD'
    THEN SUM(protocol_liquidity_usd) 
    WHEN '{{4. Currency}}' = 'eth'
    THEN SUM(protocol_liquidity_eth) 
    END AS liquidity  
FROM balancer.liquidity t
INNER JOIN dune.balancer.result_lst_tokens l 
ON t.token_address = l.contract_address
AND l.blockchain = t.blockchain
WHERE t.day >= TIMESTAMP '{{1. Start date}}'
AND t.day <= TIMESTAMP '{{2. End date}}'
AND ('{{3. Blockchain}}' = 'All' OR t.blockchain = '{{3. Blockchain}}')
GROUP BY 1,2
HAVING SUM(protocol_liquidity_usd) > 1000
ORDER BY 1 DESC, 3 DESC
