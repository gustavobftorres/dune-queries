-- part of a query repo
-- query name: Uni V2 Pool TVL
-- query link: https://dune.com/queries/4538395


WITH eth_prices AS(
    SELECT 
        DATE_TRUNC('day', minute) AS day,
        APPROX_PERCENTILE(price, .5) AS eth_price
    FROM prices.usd
    WHERE blockchain = 'base'
    AND contract_address = 0x4200000000000000000000000000000000000006
    GROUP BY 1
)

select DISTINCT
    uni.day, 
    tvl AS tvl_usd,
    tvl / eth_price AS tvl_eth
FROM "query_4527783(start='{{start}}', blockchain='{{blockchain}}', pool='{{uniswap pool}}', token_a='{{token 1}}', token_b='{{token 2}}')" uni
LEFT JOIN eth_prices eth ON uni.day = eth.day