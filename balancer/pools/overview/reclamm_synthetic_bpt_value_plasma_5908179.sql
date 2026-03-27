-- part of a query repo
-- query name: reCLAMM Synthetic BPT value Plasma
-- query link: https://dune.com/queries/5908179


WITH price_token_a_hourly AS (
    SELECT 
        date_trunc('hour', "minute") as "hour", 
        CAST(price as DOUBLE) as price
    FROM prices.usd
    WHERE 
        blockchain = '{{chain_a}}' 
        and contract_address = {{token_a}}
        AND minute > TIMESTAMP '{{start}}'
        AND minute("minute") = 0
),
price_token_b_hourly AS (
    SELECT 
        date_trunc('hour', "minute") as "hour", 
        CAST(price as DOUBLE) as price
    FROM prices.usd
    WHERE 
        blockchain = '{{chain_b}}' 
        and contract_address = {{token_b}}
        AND minute > TIMESTAMP '{{start}}'
        AND minute("minute") = 0
),
bpt_supply_hourly as (
    SELECT hour, total_supply FROM "query_5874871(pool='{{pool}}',start='{{start}}')"
),
pool_liquidity_hourly as (
    SELECT hour, token_a_balance, token_b_balance FROM "query_5875552(pool='{{pool}}',start='{{start}}')"
),
pool_volume_hourly as (
    SELECT hour, volume_usd from "query_5882946(pool='{{pool}}',start='{{start}}')"
),
virtual_balances_hourly as (
    SELECT 
        "minute" as "hour", 
        virtual_balance_a, 
        virtual_balance_b,
        "rebalancing"
    FROM "query_5892290(pool='{{pool}}')" WHERE minute("minute") = 0
),
synthetic_pool_liquidity_hourly as (
    SELECT
        L."hour",
        sqrt((virtual_balance_a + token_a_balance)*(virtual_balance_b + token_b_balance)) - virtual_balance_a as synthetic_a,
        sqrt((virtual_balance_a + token_a_balance)*(virtual_balance_b + token_b_balance)) - virtual_balance_b as synthetic_b
    FROM pool_liquidity_hourly L
    JOIN virtual_balances_hourly VB ON VB.hour = L.hour
),
bpt_prices as (
    SELECT 
        L.hour, 
        V.volume_usd,
        V.volume_usd/(PA.price*L.token_a_balance + PB.price*L.token_b_balance) as "volume_tvl",
        (PA.price*L.token_a_balance + PB.price*L.token_b_balance) as "tvl",
        (PA.price*L.token_a_balance + PB.price*token_b_balance)/BS.total_supply as bpt_price,
        (SL.synthetic_a + SL.synthetic_b)/BS.total_supply as synthetic_bpt_price, 
        PA.price as price_a, 
        PB.price as price_b, 
        token_a_balance, 
        token_b_balance, 
        total_supply
    FROM pool_liquidity_hourly L
    JOIN bpt_supply_hourly BS ON BS.hour = L.hour
    JOIN price_token_a_hourly PA ON PA.hour = L.hour
    JOIN price_token_b_hourly PB on PB.hour = L.hour
    JOIN synthetic_pool_liquidity_hourly SL ON SL.hour = L.hour
    JOIN pool_volume_hourly V on V.hour = L.hour
    WHERE L.hour > TIMESTAMP '{{start}}'
)
SELECT 
    BP.hour, 
    BP.synthetic_bpt_price,
    IF(BP.synthetic_bpt_price*VB.rebalancing = 0, NULL, BP.synthetic_bpt_price*VB.rebalancing) as rebalancing,
    BP.volume_usd,
    BP.volume_tvl,
    BP.tvl,
    BP.price_a as token_a_price,
    BP.price_b as token_b_price,
    (BP.price_a*token_a_balance)/(BP.price_a*token_a_balance + BP.price_b*token_b_balance) as weight_a,
    (BP.price_b*token_b_balance)/(BP.price_a*token_a_balance + BP.price_b*token_b_balance) as weight_b,
    token_a_balance,
    token_b_balance,
    total_supply,
    BP.bpt_price as "BPT Price"
FROM bpt_prices BP
JOIN virtual_balances_hourly VB ON BP.hour = VB.hour