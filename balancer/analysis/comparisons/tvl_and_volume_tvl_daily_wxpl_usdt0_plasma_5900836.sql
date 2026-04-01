-- part of a query repo
-- query name: TVL and Volume/TVL Daily WXPL/USDT0 - Plasma
-- query link: https://dune.com/queries/5900836


WITH price_token_a_daily AS (
    SELECT 
        date_trunc('day', "minute") as "day", 
        CAST(price as DOUBLE) as price
    FROM prices.usd
    WHERE 
        blockchain = 'bnb' 
        and contract_address = 0x405fbc9004d857903bfd6b3357792d71a50726b0
        AND minute > TIMESTAMP '2025-09-24 00:00:00'
        AND hour("minute") = 0 AND minute("minute") = 0
),
price_token_b_daily AS (
    SELECT 
        date_trunc('day', "minute") as "day", 
        CAST(price as DOUBLE) as price
    FROM prices.usd
    WHERE 
        blockchain = 'ethereum' 
        and contract_address = 0xdac17f958d2ee523a2206206994597c13d831ec7
        AND minute > TIMESTAMP '2025-09-24 00:00:00'
        AND hour("minute") = 0 AND minute("minute") = 0
),
pool_liquidity_daily as (
    SELECT hour as "day", token_a_balance, token_b_balance FROM "query_5875552()" WHERE hour("hour") = 0
),
pool_volume_daily as (
    SELECT * FROM "query_5900765()"
)
SELECT 
    L.day, 
    token_a_balance,
    token_b_balance,
    PA.price as token_a_price,
    PB.price as token_b_price,
    L.token_a_balance*PA.price + L.token_b_balance*PB.price as tvl_usd,
    V.volume_usd,
    V.volume_usd/(L.token_a_balance*PA.price + L.token_b_balance*PB.price) as "volume_tvl"
FROM pool_liquidity_daily L
JOIN pool_volume_daily V ON V.day = L.day
JOIN price_token_a_daily PA ON PA.day = L.day
JOIN price_token_b_daily PB ON PB.day = L.day