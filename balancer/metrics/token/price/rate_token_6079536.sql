-- part of a query repo
-- query name: Rate token
-- query link: https://dune.com/queries/6079536


WITH price_gno as (
    SELECT minute, price FROM prices.usd 
    WHERE contract_address = 0x6810e776880c02933d47db1b9fc05908e5386b96 and 
        blockchain = 'ethereum' and 
        minute > TIMESTAMP '{{start}}' LIMIT 1
),
price_eth as (
    SELECT minute, price FROM prices.usd 
    WHERE contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 and 
        blockchain = 'ethereum' and 
        minute > TIMESTAMP '{{start}}' LIMIT 1
)
SELECT
    G.minute,
    E.price/G.price as price
FROM price_gno G
JOIN price_eth E on E.