-- part of a query repo
-- query name: AAVE Swaps Mainnet per project
-- query link: https://dune.com/queries/6544315


WITH category_swap as (
    SELECT 
        CASE
            WHEN amount_usd < 1000 THEN 1000
            WHEN amount_usd < 5000 THEN 5000
            WHEN amount_usd < 10000 THEN 10000
            WHEN amount_usd < 20000 THEN 20000
            WHEN amount_usd < 30000 THEN 30000
            WHEN amount_usd < 40000 THEN 40000
            WHEN amount_usd < 50000 THEN 50000
            WHEN amount_usd < 60000 THEN 60000
            WHEN amount_usd < 70000 THEN 70000
            WHEN amount_usd < 80000 THEN 80000
            WHEN amount_usd < 90000 THEN 90000
            WHEN amount_usd < 100000 THEN 100000
            WHEN amount_usd < 150000 THEN 150000
            WHEN amount_usd < 200000 THEN 200000
            WHEN amount_usd < 500000 THEN 500000
            WHEN amount_usd < 1000000 THEN 1000000
            ELSE 2000000
        END as category,
        CASE
            WHEN project = 'balancer' and project_contract_address = 0x9d1fcf346ea1b073de4d5834e25572cc6ad71f4d THEN 'reCLAMM'
            WHEN project = 'balancer' THEN 'balancer other'
            WHEN project = 'uniswap' THEN 'uniswap'
            ELSE 'other'
        END as project,
        amount_usd
    FROM dex.trades 
    WHERE 
        blockchain = 'ethereum' AND 
        -- project = 'uniswap' AND 
        -- version = '3' AND
        -- project_contract_address = 0x4a79b0168296c0ef7b8f314973b82ad406a29f1b AND
        (token_bought_symbol = 'AAVE' OR token_sold_symbol = 'AAVE') AND 
        block_time >= TIMESTAMP '2026-01-18 00:00:00' AND block_time <= TIMESTAMP '2026-01-20 00:00:00'
),
total_swap as (
    SELECT
        count(*) as total_swaps,
        sum(amount_usd) as total_volume
    FROM category_swap
),
grouped_swap as (
    SELECT
        project,
        category,
        count(*) as n_swaps,
        sum(amount_usd) as volume
    FROM category_swap
    GROUP BY category, project
)
SELECT
    GS.project,
    GS.category,
    GS.n_swaps,
    GS.volume,
    TS.total_volume,
    CAST(GS.n_swaps as DOUBLE)/CAST(TS.total_swaps AS DOUBLE) as swap_percentage,
    GS.volume/TS.total_volume as volume_percentage
FROM grouped_swap GS
JOIN total_swap TS ON TS.total_swaps > 0