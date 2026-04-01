-- part of a query repo
-- query name: BPT Prices
-- query link: https://dune.com/queries/3417123


WITH liquidity AS(
    SELECT
        day,
        blockchain,
        pool_address,
        SUM(protocol_liquidity_usd) AS protocol_liquidity_usd
    FROM balancer.liquidity
    GROUP BY 1, 2, 3
)

SELECT 
    l.day,
    l.blockchain,
    s.token_address AS token,
    SUM(l.protocol_liquidity_usd / s.supply) AS price
FROM liquidity l
LEFT JOIN balancer.bpt_supply s ON l.day = s.day AND l.pool_address = s.token_address
AND l.blockchain = s.blockchain
WHERE s.supply > 0
GROUP BY 1, 2, 3