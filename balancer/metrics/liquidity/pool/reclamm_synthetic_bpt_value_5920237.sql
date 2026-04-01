-- part of a query repo
-- query name: reCLAMM Synthetic BPT value
-- query link: https://dune.com/queries/5920237


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
    SELECT date_trunc('hour', "minute") as "hour", token_a_balance, token_b_balance FROM "query_5891901(pool='{{pool}}',start='{{start}}')" WHERE minute("minute") = 0
),
pool_volume_hourly as (
    SELECT 
        date_trunc('hour', T.block_time + interval '1' hour) as "hour",
        SUM(T.amount_usd) as volume_usd
    FROM balancer_v3_multichain.vault_evt_swap S
    JOIN dex.trades T ON T.block_time > TIMESTAMP '{{start}}' 
        AND T.project = 'balancer' 
        AND T.blockchain = S.chain 
        AND S.evt_tx_hash = T.tx_hash 
        AND T.evt_index = S.evt_index
    WHERE S.pool = {{pool}}
    GROUP BY date_trunc('hour', T.block_time + interval '1' hour)
),
virtual_balances_hourly as (
    SELECT 
        "minute" as "hour", 
        virtual_balance_a, 
        virtual_balance_b,
        "rebalancing"
    FROM "query_5892290(pool='{{pool}}',start='{{start}}')" WHERE minute("minute") = 0
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
        total_supply,
        synthetic_a,
        synthetic_b
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
    IF(BP.synthetic_bpt_price*VB.rebalancing = 0, 0, BP.synthetic_bpt_price*VB.rebalancing) as rebalancing,
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
    BP.bpt_price as "BPT Price",
    VB.virtual_balance_a,
    VB.virtual_balance_b,
    synthetic_a,
    synthetic_b
FROM bpt_prices BP
JOIN virtual_balances_hourly VB ON BP.hour = VB.hour