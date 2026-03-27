-- part of a query repo
-- query name: Volume of Swaps per Swap Size (Ethereum)
-- query link: https://dune.com/queries/6350671


WITH category_swap as (
    SELECT 
        CASE
            WHEN amount_usd < 100 THEN 100
            WHEN amount_usd < 250 THEN 250
            WHEN amount_usd < 500 THEN 500
            WHEN amount_usd < 1000 THEN 1000
            WHEN amount_usd < 2000 THEN 2000
            WHEN amount_usd < 5000 THEN 5000
            WHEN amount_usd < 10000 THEN 10000
            WHEN amount_usd < 20000 THEN 20000
            WHEN amount_usd < 50000 THEN 50000
            ELSE 100000
        END as category,
        amount_usd
    FROM dex.trades WHERE 
        blockchain = '{{chain}}' AND 
        -- project = 'uniswap' AND 
        -- version = '3' AND
        -- project_contract_address != 0xa72cd899950c8fb0389a2ace09a159aca69d3383 AND
        (token_bought_symbol = '{{token}}' OR token_sold_symbol = '{{token}}') AND 
        (block_time >= TIMESTAMP '{{start_time}}' AND block_time <= TIMESTAMP '{{end_time}}')
),
total_swap as (
    SELECT
        count(*) as total_swaps,
        sum(amount_usd) as total_volume
    FROM category_swap
),
grouped_swap as (
    SELECT
        category,
        count(*) as n_swaps,
        sum(amount_usd) as volume
    FROM category_swap
    GROUP BY category
)
SELECT
    GS.category,
    GS.n_swaps,
    GS.volume,
    TS.total_volume,
    CAST(GS.n_swaps as DOUBLE)/CAST(TS.total_swaps AS DOUBLE) as swap_percentage,
    GS.volume/TS.total_volume as volume_percentage
FROM grouped_swap GS
JOIN total_swap TS ON TS.total_swaps > 0