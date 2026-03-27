-- part of a query repo
-- query name: Balancer Token Price in eth
-- query link: https://dune.com/queries/4123031


WITH eth_prices AS (
    SELECT
        DATE_TRUNC('day', minute) AS day,
        AVG(price) AS eth_price
    FROM prices.usd
    WHERE blockchain = 'ethereum'
      AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    GROUP BY 1
),
balancer_prices AS (
    SELECT 
        DATE_TRUNC('day', minute) AS day, 
        AVG(price) AS price_usd,
        AVG(price / eth_price) AS price_eth
    FROM prices.usd p
    LEFT JOIN eth_prices e ON DATE_TRUNC('day', minute) = e.day
    WHERE contract_address = 0xba100000625a3754423978a60c9317c58a424e3d
      AND DATE_TRUNC('day', minute) >= TIMESTAMP '{{start date}}'
      AND DATE_TRUNC('day', minute) <= TIMESTAMP '{{end date}}'
    GROUP BY 1
)

SELECT 
    day,
    price_eth,
    -- 30-day SMA
    AVG(price_eth) OVER (
        ORDER BY day 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS sma_30d,
    -- 90-day SMA
    AVG(price_eth) OVER (
        ORDER BY day 
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS sma_90d
FROM balancer_prices
ORDER BY day;