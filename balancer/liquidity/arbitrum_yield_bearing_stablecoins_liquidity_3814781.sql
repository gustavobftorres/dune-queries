-- part of a query repo
-- query name: Arbitrum Yield-Bearing Stablecoins Liquidity
-- query link: https://dune.com/queries/3814781


SELECT 
    CAST(day AS TIMESTAMP) AS day, 
    l.token_symbol, 
    CASE WHEN '{{Stablecoins TVL Currency}}' = 'USD'
            THEN SUM(protocol_liquidity_usd) 
        WHEN '{{Stablecoins TVL Currency}}' = 'ETH'
            THEN SUM(protocol_liquidity_eth) 
        END AS liquidity  
FROM balancer.liquidity t
INNER JOIN query_3814790 l 
ON t.token_address = l.token_address
AND l.blockchain = t.blockchain
WHERE t.day >= TIMESTAMP '{{Start date}}'
AND t.day <= TIMESTAMP '{{End date}}'
AND t.blockchain = 'arbitrum'
GROUP BY 1,2
ORDER BY 1 DESC, 3 DESC