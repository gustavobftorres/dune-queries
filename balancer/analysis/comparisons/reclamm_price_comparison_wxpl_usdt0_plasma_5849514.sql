-- part of a query repo
-- query name: reCLAMM Price Comparison - WXPL/USDT0 Plasma
-- query link: https://dune.com/queries/5849514


WITH pool_tokens AS (
    SELECT 
        pool,
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[1]), '$.token'), 3)) as token_a, 
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[2]), '$.token'), 3)) as token_b 
    FROM balancer_v3_plasma.vault_evt_poolregistered where pool = 0xe14ba497a7c51f34896d327ec075f3f18210a270
),
price_chain_A AS (
    SELECT minute, CAST(price as DOUBLE) as price
    FROM prices.usd
    WHERE 
        blockchain = 'bnb' 
        and contract_address = 0x405fbc9004d857903bfd6b3357792d71a50726b0
        and minute > now() - interval '3' day
        and minute < now()
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
price_reclamm AS (
    SELECT 
        date_trunc('minute', S.evt_block_time + interval '30' second) as "minute",
        CASE
            WHEN S.tokenIn = PT.token_a THEN CAST(S.amountOut as DOUBLE) * 1e12 / CAST(S.amountIn as DOUBLE)
            ELSE CAST(S.amountIn as DOUBLE) * 1e12 / CAST(S.amountOut as DOUBLE)
        END AS price
    FROM balancer_v3_plasma.vault_evt_swap S
    JOIN pool_tokens PT ON PT.pool = S.pool
    WHERE S.pool = 0xe14ba497a7c51f34896d327ec075f3f18210a270 
        AND S.evt_block_time > now() - interval '3' day
        AND S.evt_block_time < now()
),
price_reclamm_all_minutes AS (
    SELECT 
        B.minute,
        PR.price
    FROM price_chain_B B 
    LEFT JOIN price_reclamm PR ON PR.minute = B.minute
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
)
SELECT
    A."minute",
    A.price/B.price as "Market",
    PR.price as "reCLAMM"
FROM price_chain_A AS A
JOIN price_chain_B AS B on A."minute" = B."minute"
LEFT JOIN price_reclamm_filled AS PR on A."minute" = PR."minute"
ORDER BY "minute"