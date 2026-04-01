-- part of a query repo
-- query name: ETH price
-- query link: https://dune.com/queries/3387710


SELECT 
    date_trunc('day', minute) AS day,
    APPROX_PERCENTILE(price, 0.5) AS median_price_eth
FROM prices.usd
    WHERE symbol = 'WETH'
        AND blockchain = 'ethereum'
GROUP BY 1
