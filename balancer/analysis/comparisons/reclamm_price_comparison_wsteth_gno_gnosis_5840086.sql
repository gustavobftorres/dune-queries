-- part of a query repo
-- query name: reCLAMM Price Comparison - wstETH/GNO Gnosis
-- query link: https://dune.com/queries/5840086


WITH balancer_price AS (
    SELECT
        *
    FROM "query_5757256(pool_balancer='0xa50085ff1dfa173378e7d26a76117d68d5eba539',pool_aero='0x000000000000000000000000000000000000000000',token_a='0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0',token_b='0x6810e776880c02933d47db1b9fc05908e5386b96')"
),
price_uni_v3 AS (
    SELECT
        date_trunc('minute', block_time) as "minute",
        MAX(CASE
            WHEN token_bought_symbol = 'GNO' THEN token_bought_amount/token_sold_amount
            ELSE token_sold_amount/token_bought_amount
        END) as price
    FROM uniswap.trades 
    WHERE project_contract_address = 0x46f2da8a69a150390a87db78e7aad8572c564963
        AND block_time > TIMESTAMP '2025-10-01 00:00:00' 
    GROUP BY date_trunc('minute', block_time)
),
price_uni_v3_all_minutes AS (
    SELECT 
        A.minute,
        PU.price
    FROM balancer_price A 
    LEFT JOIN price_uni_v3 PU ON PU.minute = A.minute
),
uni_v3_flagged AS (
    SELECT *,
        SUM(CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END) 
            OVER (ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM price_uni_v3_all_minutes
),
price_uni_v3_filled AS (
    SELECT 
        minute,
        MAX(price) OVER (PARTITION BY grp ORDER BY minute) AS price
    FROM uni_v3_flagged
)
SELECT
    BP.*,
    PU.price as "Uni V3"
FROM balancer_price BP
LEFT JOIN price_uni_v3_filled PU ON PU.minute = BP.minute
WHERE BP."minute" >= date_trunc('day', now() - interval '10' day)