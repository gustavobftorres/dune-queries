-- part of a query repo
-- query name: Balancer Token Price
-- query link: https://dune.com/queries/2828018


WITH eth_prices AS (
        SELECT
            DATE_TRUNC('day', minute) as day,
            AVG(price) as eth_price
        FROM prices.usd
        WHERE blockchain = 'ethereum'
        AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        GROUP BY 1
    )

SELECT 
    date_trunc('day', minute) AS day, 
    AVG(price) AS "Price",
    AVG(price / eth_price) AS price_eth
FROM prices.usd p
LEFT JOIN eth_prices e ON date_trunc('day', minute) = e.day
WHERE contract_address = 0xba100000625a3754423978a60c9317c58a424e3d
GROUP BY 1