-- part of a query repo
-- query name: Parameterized reCLAMM Liquidity Depth
-- query link: https://dune.com/queries/5830780


WITH pool_tokens AS (
    SELECT 
        pool,
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[1]), '$.token'), 3)) as token_a, 
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[2]), '$.token'), 3)) as token_b 
    FROM balancer_v3_multichain.vault_evt_poolregistered where chain = '{{chain}}' AND pool = {{pool}}
),
price_chain_A AS (
    SELECT minute, CAST(price as DOUBLE) as price
    FROM prices.usd 
    WHERE 
        blockchain = '{{chain_a}}'
        and contract_address = {{token_a}}
        and minute > TIMESTAMP '{{start}}'
        and minute < TIMESTAMP '{{end}}'
),
reclamm_liquidity_minute as (
    SELECT 
        * 
    FROM "query_5891901(pool='{{pool}}',start='{{start}}',end='{{end}}')"
),
reclamm_virtual_balances_minute as (
    SELECT * FROM "query_5892290(pool='{{pool}}',start='{{start}}',end='{{end}}')"
),
pool_state_on_each_swap AS (
    SELECT
        L."minute",
        L.token_a_balance,
        L.token_b_balance,
        VB.virtual_balance_a,
        VB.virtual_balance_b,
        (L.token_a_balance + VB.virtual_balance_a) * (L.token_b_balance + VB.virtual_balance_b) as invariant,
        IF(
            (L.token_a_balance + VB.virtual_balance_a) = 0, 
            0, 
            (L.token_b_balance + VB.virtual_balance_b) / (L.token_a_balance + VB.virtual_balance_a)
        ) as spot_price
    FROM reclamm_liquidity_minute L
    JOIN reclamm_virtual_balances_minute VB ON VB.minute = L.minute
)
SELECT
    S."minute",
    A.price * (sqrt(invariant / (0.98*spot_price)) - token_a_balance - virtual_balance_a) as liquidity_depth_a
FROM pool_state_on_each_swap S
JOIN price_chain_A A ON A.minute = S.minute
ORDER BY S."minute"