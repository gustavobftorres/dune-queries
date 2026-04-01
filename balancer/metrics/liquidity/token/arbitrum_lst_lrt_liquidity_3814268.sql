-- part of a query repo
-- query name: Arbitrum LST/LRT Liquidity
-- query link: https://dune.com/queries/3814268


SELECT 
    CAST(day AS TIMESTAMP) AS day, 
    l.symbol, 
    CASE WHEN '{{LST TVL Currency}}' = 'USD'
            THEN SUM(protocol_liquidity_usd) 
        WHEN '{{LST TVL Currency}}' = 'ETH'
            THEN SUM(protocol_liquidity_eth) 
        END AS liquidity
FROM balancer.liquidity t
INNER JOIN dune.balancer.result_lst_tokens l 
ON t.token_address = l.contract_address
AND l.blockchain = t.blockchain
WHERE t.day >= TIMESTAMP '{{Start date}}'
AND t.day <= TIMESTAMP '{{End date}}'
AND t.blockchain = 'arbitrum'
AND l.symbol != 'SOL'
GROUP BY 1,2
HAVING SUM(protocol_liquidity_usd) > 1000
ORDER BY 1 DESC, 3 DESC
