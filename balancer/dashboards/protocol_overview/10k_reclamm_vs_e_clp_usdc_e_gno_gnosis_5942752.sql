-- part of a query repo
-- query name: 10K reCLAMM vs E-CLP USDC.e/GNO - Gnosis
-- query link: https://dune.com/queries/5942752


WITH bpt_prices_reclamm as (
    SELECT L.day, L.pool_address, SUM(L.pool_liquidity_usd)/MAX(BS.supply) as bpt_price
    FROM balancer.liquidity L
    JOIN balancer.bpt_supply BS ON BS.day > now() - interval '30' day 
        AND BS.token_address = L.pool_address 
        AND BS.day = L.day 
    WHERE L.day >= TIMESTAMP '2025-09-25 00:00:00' 
        AND L.pool_address = {{pool}}
    GROUP BY L.day, L.pool_address
),
initial_bpt_price_reclamm AS (
    SELECT 
        pool_address as pool,
        bpt_price as initial_bpt_price
    FROM bpt_prices_reclamm
    ORDER BY day
    LIMIT 1
),
bpt_prices_eclp as (
    SELECT L.day, L.pool_address, SUM(L.pool_liquidity_usd)/MAX(BS.supply) as bpt_price
    FROM balancer.liquidity L
    JOIN balancer.bpt_supply BS ON BS.day > now() - interval '30' day 
        AND BS.token_address = L.pool_address 
        AND BS.day = L.day 
    WHERE L.day >= TIMESTAMP '2025-09-25 00:00:00'
        AND L.pool_address = 0x48094f85aeeb2d67d6f1ef2409d600c02859e57c
    GROUP BY L.day, L.pool_address
),
initial_bpt_price_eclp AS (
    SELECT 
        pool_address as pool,
        bpt_price as initial_bpt_price
    FROM bpt_prices_eclp
    ORDER BY day
    LIMIT 1
),
initial_token_a_price AS (
    SELECT 
        {{pool}} as pool,
        "timestamp" as day,
        "price"
    FROM prices.day WHERE blockchain = '{{chain_a}}' 
        AND "timestamp" >= TIMESTAMP '2025-09-25 00:00:00'  
        AND contract_address = {{token_a}}
    ORDER BY "timestamp" LIMIT 1
),
initial_token_b_price AS (
    SELECT 
        {{pool}} as pool,
        "timestamp" as day,
        "price"
    FROM prices.day WHERE blockchain = '{{chain_b}}' 
        AND "timestamp" >= TIMESTAMP '2025-09-25 00:00:00' 
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
    10000*BP.bpt_price/IBP.initial_bpt_price as "reCLAMM",
    10000*BPE.bpt_price/IBPE.initial_bpt_price as "E-CLP",
    IBP.initial_bpt_price,
    TA.price as token_a_price,
    TB.price as token_b_price,
    ITA.price as initial_token_a_price,
    ITB.price as initial_token_b_price,
    10000/(2*ITA.price) as hodl_a_balance,
    10000/(2*ITB.price) as hodl_b_balance,
    10000/(2*ITA.price)*TA.price + 10000/(2*ITB.price)*TB.price AS "Weighted Hodl",
    ((10000/(2*ITA.price)*TA.price + 10000/(2*ITB.price)*TB.price) - 10000*BP.bpt_price/IBP.initial_bpt_price)/(10000*BP.bpt_price/IBP.initial_bpt_price) as "Impermanent Loss" 
FROM bpt_prices_reclamm BP
JOIN initial_bpt_price_reclamm IBP ON IBP.pool = BP.pool_address
JOIN bpt_prices_eclp BPE ON BPE.day = BP.day
JOIN initial_bpt_price_eclp IBPE ON IBPE.pool = BPE.pool_address
JOIN initial_token_a_price ITA ON ITA.pool = BP.pool_address
JOIN initial_token_b_price ITB ON ITB.pool = BP.pool_address
JOIN token_a_price TA ON TA.day = BP.day
JOIN token_b_price TB ON TB.day = BP.day