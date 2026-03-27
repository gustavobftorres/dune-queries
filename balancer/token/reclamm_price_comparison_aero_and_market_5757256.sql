-- part of a query repo
-- query name: reCLAMM Price Comparison (Aero and Market)
-- query link: https://dune.com/queries/5757256


WITH pool_tokens AS (
    SELECT 
        pool,
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[1]), '$.token'), 3)) as token_a, 
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[2]), '$.token'), 3)) as token_b 
    FROM balancer_v3_multichain.vault_evt_poolregistered where pool = {{pool_balancer}}
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
price_chain_B AS (
    SELECT minute, CAST(price as DOUBLE) as price
    FROM prices.usd
    WHERE 
        blockchain = '{{chain_b}}' 
        and contract_address = {{token_b}}
        and minute > TIMESTAMP '{{start}}'
        and minute < TIMESTAMP '{{end}}'
),
price_aero AS (
    SELECT 
        date_trunc('minute', evt_block_time + interval '30' second) as "minute", 
        MAX(CAST(reserve1 AS DOUBLE)/CAST(reserve0 AS DOUBLE)) as price
    FROM aerodrome_base.pool_evt_sync 
    WHERE contract_address = {{pool_aero}}
       AND evt_block_time > TIMESTAMP '{{start}}'
        and evt_block_time < TIMESTAMP '{{end}}'
    GROUP BY date_trunc('minute', evt_block_time + interval '30' second)
),
price_aero_all_minutes AS (
    SELECT 
        A.minute,
        PA.price
    FROM price_chain_A A 
    LEFT JOIN price_aero PA ON PA.minute = A.minute
),
aero_flagged AS (
    SELECT *,
        SUM(CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END) 
            OVER (ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM price_aero_all_minutes
),
price_aero_filled AS (
    SELECT 
        minute,
        MAX(price) OVER (PARTITION BY grp ORDER BY minute) AS price
    FROM aero_flagged
),
price_reclamm AS (
    SELECT 
        date_trunc('minute', T.block_time + interval '30' second) as "minute",
        MAX(CAST(CASE
            WHEN T.token_bought_address = PT.token_a THEN T.token_sold_amount / T.token_bought_amount
            ELSE T.token_bought_amount / T.token_sold_amount
        END AS DOUBLE)) AS price
    FROM balancer_v3_multichain.vault_evt_swap S
    JOIN pool_tokens PT ON PT.pool = S.pool
    JOIN dex.trades T ON T.block_time > TIMESTAMP '{{start}}' AND T.project = 'balancer' AND T.blockchain = S.chain AND S.evt_tx_hash = T.tx_hash and T.evt_index = S.evt_index
    WHERE S.pool = {{pool_balancer}} AND S.evt_block_time > TIMESTAMP '{{start}}' and S.evt_block_time < TIMESTAMP '{{end}}'
    GROUP BY date_trunc('minute', T.block_time + interval '30' second)
),
price_reclamm_all_minutes AS (
    SELECT 
        A.minute,
        PR.price
    FROM price_chain_A A 
    LEFT JOIN price_reclamm PR ON PR.minute = A.minute
),
reclamm_flagged AS (
    SELECT *,
        SUM(CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END) 
            OVER (ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM price_reclamm_all_minutes
),
price_reclamm_filled AS (
    SELECT 
        minute,
        MAX(price) OVER (PARTITION BY grp ORDER BY minute) AS price
    FROM reclamm_flagged
),
reclamm_liquidity_minute as (
    SELECT 
        * 
    FROM "query_5891901(pool='{{pool_balancer}}',start='{{start}}',end='{{end}}')"
),
reclamm_virtual_balances_minute as (
    SELECT * FROM "query_5892290(pool='{{pool_balancer}}',start='{{start}}',end='{{end}}')"
),
price_reclamm_spot as (
    SELECT
        L.minute,
        L.token_a_balance,
        L.token_b_balance,
        VB.virtual_balance_a,
        VB.virtual_balance_b,
        IF(
            (L.token_a_balance + VB.virtual_balance_a) = 0, 
            0, 
            (L.token_b_balance + VB.virtual_balance_b)/(L.token_a_balance + VB.virtual_balance_a)
        ) as price
    FROM reclamm_liquidity_minute L
    JOIN reclamm_virtual_balances_minute VB ON VB.minute = L.minute
),
price_swapr AS (
    SELECT 
        date_trunc('minute', evt_block_time) as "minute",
        CAST(reserve0 as DOUBLE)/CAST(reserve1 as DOUBLE) as price
    FROM swapr_gnosis.dxswappair_evt_sync 
    WHERE contract_address = 0x8028457e452d7221db69b1e0563aa600a059fab1 
        AND evt_block_time > TIMESTAMP '{{start}}' 
),
price_swapr_all_minutes AS (
    SELECT 
        A.minute,
        PR.price
    FROM price_chain_A A 
    LEFT JOIN price_swapr PR ON PR.minute = A.minute
),
swapr_flagged AS (
    SELECT *,
        SUM(CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END) 
            OVER (ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM price_swapr_all_minutes
),
price_swapr_filled AS (
    SELECT 
        minute,
        MAX(price) OVER (PARTITION BY grp ORDER BY minute) AS price
    FROM swapr_flagged
),
price_uni_v2 AS (
    SELECT
        date_trunc('minute', evt_block_time) as "minute",
        CAST(reserve1 as DOUBLE)/CAST(reserve0 as DOUBLE) as price
    FROM uniswap_v2_multichain.uniswapv2pair_evt_sync 
    WHERE contract_address = {{pool_uni_v2}}
        AND evt_block_time > TIMESTAMP '{{start}}' 
),
price_uni_v2_all_minutes AS (
    SELECT 
        A.minute,
        PU.price
    FROM price_chain_A A 
    LEFT JOIN price_uni_v2 PU ON PU.minute = A.minute
),
uni_v2_flagged AS (
    SELECT *,
        SUM(CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END) 
            OVER (ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM price_uni_v2_all_minutes
),
price_uni_v2_filled AS (
    SELECT 
        minute,
        MAX(price) OVER (PARTITION BY grp ORDER BY minute) AS price
    FROM uni_v2_flagged
)
SELECT
    A."minute",
    A.price/B.price as "Market",
    A.price as "price_a",
    B.price as "price_b",
    PA.price as "Aero (Base)",
    PR.price as "reCLAMM",
    PRF.price as "reCLAMM (Trades)",
    PS.price as "Swapr (Gnosis)",
    PU2.price as "Uni V2",
    token_a_balance,
    token_b_balance,
    virtual_balance_a,
    virtual_balance_b
FROM price_chain_A AS A
JOIN price_chain_B AS B on A."minute" = B."minute"
LEFT JOIN price_aero_filled AS PA on A."minute" = PA."minute"
LEFT JOIN price_reclamm_spot AS PR on A."minute" = PR."minute"
LEFT JOIN price_reclamm_filled AS PRF on A."minute" = PRF."minute"
LEFT JOIN price_swapr_filled AS PS on A."minute" = PS."minute"
LEFT JOIN price_uni_v2_filled AS PU2 on A."minute" = PU2."minute"
ORDER BY "minute"
