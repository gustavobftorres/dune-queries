-- part of a query repo
-- query name: Daily TVL on Arbitrum
-- query link: https://dune.com/queries/3222881


SELECT CAST(day AS TIMESTAMP) AS day, 
        CASE WHEN '{{General TVL Currency}}' = 'USD'
            THEN SUM(protocol_liquidity_usd) 
        WHEN '{{General TVL Currency}}' = 'ETH'
            THEN SUM(protocol_liquidity_eth) 
        END AS tvl
FROM balancer.liquidity
WHERE blockchain = 'arbitrum'
AND day <= TIMESTAMP '{{End date}}'
AND day >= date_trunc('year', TIMESTAMP '{{Start date}}')
GROUP BY 1