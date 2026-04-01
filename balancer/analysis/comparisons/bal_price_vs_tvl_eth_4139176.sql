-- part of a query repo
-- query name: BAL Price vs. TVL (ETH)
-- query link: https://dune.com/queries/4139176


WITH eth_price AS(
SELECT 
    DATE_TRUNC('day', minute) AS day,
    APPROX_PERCENTILE(price, 0.5) AS eth_price
FROM prices.usd
WHERE blockchain = 'ethereum'
AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
GROUP BY 1
),

bal_price AS(
SELECT 
    DATE_TRUNC('day', minute) AS day,
    APPROX_PERCENTILE(price, 0.5) AS bal_price
FROM prices.usd
WHERE blockchain = 'ethereum'
AND symbol = 'BAL'
GROUP BY 1
)

SELECT
    l.day,
    SUM(protocol_liquidity_eth) AS tvl_eth,
    SUM(protocol_liquidity_usd) AS tvl_usd,
    bal_price / eth_price AS bal_price_eth
FROM balancer.liquidity l
JOIN eth_price e ON l.day = e.day
JOIN bal_price b ON l.day = b.day
WHERE l.day >= TIMESTAMP '{{start date}}'
AND l.day <= TIMESTAMP '{{end date}}'
GROUP BY 1, 4