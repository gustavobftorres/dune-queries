-- part of a query repo
-- query name: Parameterized reCLAMM IL
-- query link: https://dune.com/queries/5840683


WITH bpt_prices as (
    SELECT L.day, L.pool_address, SUM(L.pool_liquidity_usd)/MAX(BS.supply) as bpt_price
    FROM balancer.liquidity L
    JOIN balancer.bpt_supply BS ON BS.day > now() - interval '30' day 
        AND BS.token_address = L.pool_address 
        AND BS.day = L.day 
    WHERE L.day > now() - interval '30' day 
        AND L.pool_address = {{pool}}
    GROUP BY L.day, L.pool_address
),
initial_bpt_price AS (
    SELECT 
        pool_address as pool,
        bpt_price as initial_bpt_price
    FROM bpt_prices
    WHERE day > now() - interval '30' day 
    ORDER BY day
    LIMIT 1
),
initial_token_a_price AS (
    SELECT 
        {{pool}} as pool,
        "timestamp" as day,
        "price"
    FROM prices.day WHERE blockchain = '{{chain_a}}' 
        AND "timestamp" > now() - interval '30' day 
        AND contract_address = {{token_a}}
    ORDER BY "timestamp" LIMIT 1
),
initial_token_b_price AS (
    SELECT 
        {{pool}} as pool,
        "timestamp" as day,
        "price"
    FROM prices.day WHERE blockchain = '{{chain_b}}' 
        AND "timestamp" > now() - interval '30' day 
        AND contract_address = {{token_b}}
    ORDER BY "timestamp" LIMIT 1
),
token_a_price AS (
    SELECT 
        "timestamp" as day,
        "price"
    FROM prices.day WHERE blockchain = '{{chain_a}}' 
        AND "timestamp" > now() - interval '30' day 
        AND contract_address = {{token_a}}
),
token_b_price AS (
    SELECT 
        "timestamp" as day,
        "price"
    FROM prices.day WHERE blockchain = '{{chain_b}}' 
        AND "timestamp" > now() - interval '30' day 
        AND contract_address = {{token_b}}
)
SELECT 
    BP.day, 
    BP.bpt_price as "BPT Price",
    IBP.initial_bpt_price,
    TA.price as token_a_price,
    TB.price as token_b_price,
    ITA.price as initial_token_a_price,
    ITB.price as initial_token_b_price,
    IBP.initial_bpt_price/(2*ITA.price) as hodl_a_balance,
    IBP.initial_bpt_price/(2*ITB.price) as hodl_b_balance,
    IBP.initial_bpt_price/(2*ITA.price)*TA.price + IBP.initial_bpt_price/(2*ITB.price)*TB.price AS "Weighted Hodl",
    ((IBP.initial_bpt_price/(2*ITA.price)*TA.price + IBP.initial_bpt_price/(2*ITB.price)*TB.price) - BP.bpt_price)/BP.bpt_price as "Impermanent Loss" 
FROM bpt_prices BP
JOIN initial_bpt_price IBP ON IBP.pool = BP.pool_address
JOIN initial_token_a_price ITA ON ITA.pool = BP.pool_address
JOIN initial_token_b_price ITB ON ITB.pool = BP.pool_address
JOIN token_a_price TA ON TA.day = BP.day
JOIN token_b_price TB ON TB.day = BP.day