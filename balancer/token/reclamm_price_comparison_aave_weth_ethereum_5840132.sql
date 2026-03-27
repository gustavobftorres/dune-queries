-- part of a query repo
-- query name: reCLAMM Price Comparison - AAVE/WETH Ethereum
-- query link: https://dune.com/queries/5840132


WITH balancer_price AS (
    SELECT
        *
    FROM "query_5757256(pool_balancer='0x9d1fcf346ea1b073de4d5834e25572cc6ad71f4d',pool_aero='0x0000000000000000000000000000000000000000',token_a='0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',token_b='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')"
),
price_uni_v3 AS (
    SELECT
        date_trunc('minute', block_time) as "minute",
        CASE
            WHEN token_bought_symbol = 'AAVE' THEN token_sold_amount/token_bought_amount
            ELSE token_bought_amount/token_sold_amount
        END as price
    FROM uniswap_v3_ethereum.trades 
    WHERE project_contract_address = 0x5aB53EE1d50eeF2C1DD3d5402789cd27bB52c1bB
        AND block_time > TIMESTAMP '2025-10-01 00:00:00' 
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
),
price_aero_cl AS (
    SELECT 
        date_trunc('minute', evt_block_time + interval '30' second) as "minute", 
        -MAX(CAST(amount0 AS DOUBLE)/CAST(amount1 AS DOUBLE)) as price
    FROM aerodrome_base.clpool_evt_swap 
    WHERE contract_address = 0x4a79B0168296c0eF7b8F314973B82aD406a29f1B
       AND evt_block_time > TIMESTAMP '2025-10-01 00:00:00'
    GROUP BY date_trunc('minute', evt_block_time + interval '30' second)
),
price_aero_cl_all_minutes AS (
    SELECT 
        A.minute,
        MAX(PA.price) as price
    FROM balancer_price A 
    LEFT JOIN price_aero_cl PA ON PA.minute = A.minute
    GROUP BY A.minute
),
aero_cl_flagged AS (
    SELECT *,
        SUM(CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END) 
            OVER (ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM price_aero_cl_all_minutes
),
price_aero_cl_filled AS (
    SELECT 
        minute,
        MAX(price) OVER (PARTITION BY grp ORDER BY minute) AS price
    FROM aero_cl_flagged
)
SELECT
    BP.*,
    PU.price as "Uni V3",
    PACL.price as "Aero CL"
FROM balancer_price BP
LEFT JOIN price_uni_v3_filled PU ON PU.minute = BP.minute
LEFT JOIN price_aero_cl_filled PACL ON PACL.minute = BP.minute
WHERE BP."minute" >= date_trunc('day', now() - interval '2' day)