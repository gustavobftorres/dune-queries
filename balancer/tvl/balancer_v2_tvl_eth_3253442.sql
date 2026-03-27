-- part of a query repo
-- query name: Balancer V2 TVL (ETH)
-- query link: https://dune.com/queries/3253442


WITH eth_price AS (
SELECT date_trunc('day', minute) day, AVG(price) as price
FROM prices.usd
WHERE symbol = 'ETH'
GROUP BY 1
)

SELECT p.day, SUM(protocol_liquidity_usd) AS tvl, p.price, SUM(protocol_liquidity_usd)/p.price AS tvl_eth
FROM balancer.liquidity l
LEFT JOIN eth_price p ON l.day = p.day 
WHERE l.version = '2'
AND CAST(l.day AS TIMESTAMP) > CAST('{{Start Date}}' AS TIMESTAMP)
AND ('{{Blockchain}}' = 'All' OR blockchain = '{{Blockchain}}')
GROUP BY 1, 3
HAVING SUM(protocol_liquidity_usd) < 1e9
ORDER BY 1 DESC
