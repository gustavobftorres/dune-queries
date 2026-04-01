-- part of a query repo
-- query name: reCLAMM Price Comparison - sUSDai/USDT0 Plasma
-- query link: https://dune.com/queries/5894389


WITH pool_tokens AS (
    SELECT 
        pool,
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[1]), '$.token'), 3)) as token_a, 
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[2]), '$.token'), 3)) as token_b 
    FROM balancer_v3_plasma.vault_evt_poolregistered where pool = 0xb3ca3ead1c59ded552cd30a6992038284b418b65
),
price_chain_A AS (
    SELECT date_trunc('minute', "timestamp") as minute, CAST(price as DOUBLE) as price
    FROM prices.minute
    WHERE 
        blockchain = 'arbitrum' 
        and contract_address = 0x0b2b2b2076d95dda7817e785989fe353fe955ef9
        and "timestamp" > now() - interval '3' day
        and "timestamp" < now()
),
price_chain_B AS (
    SELECT minute, CAST(price as DOUBLE) as price
    FROM prices.usd
    WHERE 
        blockchain = 'ethereum' 
        and contract_address = 0xdac17f958d2ee523a2206206994597c13d831ec7
        and minute > now() - interval '3' day
        and minute < now()
),
reclamm_liquidity_minute as (
    SELECT 
        * 
    FROM "query_5891901(pool='0xb3ca3ead1c59ded552cd30a6992038284b418b65',start='2025-09-25 00:00:00')"
),
reclamm_virtual_balances_minute as (
    SELECT * FROM "query_5892290(pool='0xb3ca3ead1c59ded552cd30a6992038284b418b65',start='2025-09-25 00:00:00')"
),
price_reclamm_spot as (
    SELECT
        L.minute,
        IF(
            (L.token_a_balance + VB.virtual_balance_a) = 0, 
            0, 
            (L.token_b_balance + VB.virtual_balance_b)/(L.token_a_balance + VB.virtual_balance_a)
        ) as price
    FROM reclamm_liquidity_minute L
    JOIN reclamm_virtual_balances_minute VB ON VB.minute = L.minute
)
SELECT
    A."minute",
    A.price/B.price as "Market",
    PR.price as "reCLAMM"
FROM price_chain_A AS A
JOIN price_chain_B AS B on A."minute" = B."minute"
LEFT JOIN price_reclamm_spot AS PR on A."minute" = PR."minute"
ORDER BY "minute"