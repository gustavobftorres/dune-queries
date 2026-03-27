-- part of a query repo
-- query name: reCLAMM IL sUSDai/WXPL - Plasma
-- query link: https://dune.com/queries/5907914


WITH price_token_a_hourly AS (
    SELECT 
        date_trunc('hour', "timestamp") as "hour", 
        CAST(price as DOUBLE) as price
    FROM prices.minute
    WHERE 
        blockchain = 'arbitrum' 
        and contract_address = 0x0b2b2b2076d95dda7817e785989fe353fe955ef9
        AND "timestamp" > TIMESTAMP '2025-09-24 00:00:00'
        AND minute("timestamp") = 0
),
price_token_b_hourly AS (
    SELECT 
        date_trunc('hour', "minute") as "hour", 
        CAST(price as DOUBLE) as price
    FROM prices.usd
    WHERE 
        blockchain = 'bnb' 
        and contract_address = 0x405fbc9004d857903bfd6b3357792d71a50726b0
        AND minute > TIMESTAMP '2025-09-24 00:00:00'
        AND minute("minute") = 0
),
bpt_supply_hourly as (
    SELECT hour, total_supply FROM "query_5874871(pool='0x62544ba60a02d6ab1dc97daf5098ac18d4fc31cf')"
),
pool_liquidity_hourly as (
    SELECT hour, token_a_balance, token_b_balance FROM "query_5875552(pool='0x62544ba60a02d6ab1dc97daf5098ac18d4fc31cf',decimals_b='1e18')"
),
bpt_prices as (
    SELECT 
        L.hour, 
        PA.price*L.token_a_balance + PB.price*token_b_balance as pool_tvl, 
        100000*(PA.price*L.token_a_balance + PB.price*token_b_balance)/BS.total_supply as bpt_price, 
        PA.price as price_a, 
        PB.price as price_b, 
        token_a_balance, 
        token_b_balance, 
        total_supply
    FROM pool_liquidity_hourly L
    JOIN bpt_supply_hourly BS ON BS.hour = L.hour
    JOIN price_token_a_hourly PA ON PA.hour = L.hour
    JOIN price_token_b_hourly PB on PB.hour = L.hour
    WHERE L.hour > TIMESTAMP '2025-09-24 00:00'
),
initial_bpt_price AS (
    SELECT
        bpt_price as initial_bpt_price
    FROM bpt_prices
    ORDER BY hour
    LIMIT 1
),
initial_token_a_price AS (
    SELECT 
        "price_a" as price
    FROM bpt_prices
    ORDER BY "hour" LIMIT 1
),
initial_token_b_price AS (
    SELECT 
        "price_b" as price
    FROM bpt_prices
    ORDER BY "hour" LIMIT 1
)
SELECT 
    BP.hour, 
    BP.pool_tvl,
    BP.bpt_price as "BPT Price",
    BP.bpt_price/(
        (BP.price_a*BP.price_a*token_a_balance + BP.price_b*BP.price_b*token_b_balance)/(BP.price_a*token_a_balance + BP.price_b*token_b_balance)
    ) as "crazy metric",
    IBP.initial_bpt_price,
    BP.price_a as token_a_price,
    BP.price_b as token_b_price,
    BP.price_a/BP.price_b as relation_tokens,
    ITA.price as initial_token_a_price,
    ITB.price as initial_token_b_price,
    (BP.price_a*token_a_balance)/(BP.price_a*token_a_balance + BP.price_b*token_b_balance) as weight_a,
    (BP.price_b*token_b_balance)/(BP.price_a*token_a_balance + BP.price_b*token_b_balance) as weight_b,
    token_a_balance,
    token_b_balance,
    total_supply,
    IBP.initial_bpt_price/(2*ITA.price) as hodl_a_balance,
    IBP.initial_bpt_price/(2*ITB.price) as hodl_b_balance,
    IBP.initial_bpt_price/(2*ITA.price)*BP.price_a + IBP.initial_bpt_price/(2*ITB.price)*BP.price_b AS "Weighted Hodl",
    ((IBP.initial_bpt_price/(2*ITA.price)*BP.price_a + IBP.initial_bpt_price/(2*ITB.price)*BP.price_b) - BP.bpt_price)/BP.bpt_price as "Impermanent Loss" 
FROM bpt_prices BP
JOIN initial_bpt_price IBP ON IBP.initial_bpt_price IS NOT NULL
JOIN initial_token_a_price ITA ON ITA.price IS NOT NULL
JOIN initial_token_b_price ITB ON ITB.price IS NOT NULL