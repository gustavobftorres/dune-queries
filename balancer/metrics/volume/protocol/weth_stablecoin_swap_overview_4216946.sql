-- part of a query repo
-- query name: WETH <> Stablecoin Swap Overview
-- query link: https://dune.com/queries/4216946


WITH swaps AS (
    SELECT
        blockchain,
        token_pair,
        CASE 
            WHEN token_bought_symbol IN ('USDC', 'USDT', 'DAI', 'GYD', 'GHO', 'mUSD', 'sUSD', 'rETH', 'wstETH') THEN token_bought_symbol
            WHEN token_sold_symbol IN ('USDC', 'USDT', 'DAI', 'GYD', 'GHO', 'mUSD', 'sUSD', 'rETH', 'wstETH') THEN token_sold_symbol
        END AS stablecoin,
        amount_usd,
        DATE_TRUNC('day', block_time) AS swap_day,
        DATE_TRUNC('week', block_time) AS swap_week,
        DATE_TRUNC('month', block_time) AS swap_month
    FROM balancer.trades
    WHERE ((token_pair LIKE '%WETH%'
      AND (token_bought_symbol IN ('USDC', 'USDT', 'DAI', 'GYD', 'GHO', 'mUSD', 'sUSD', 'rETH', 'wstETH')
           OR token_sold_symbol IN ('USDC', 'USDT', 'DAI', 'GYD', 'GHO', 'mUSD', 'sUSD', 'rETH', 'wstETH')
           ))
           OR (token_bought_symbol IN ('USDC', 'USDT', 'DAI')
           AND token_sold_symbol IN ('USDC', 'USDT', 'DAI')))
      AND ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
      AND block_month >= TIMESTAMP '{{Start Date}}'
      AND block_month <= TIMESTAMP '{{End Date}}'
),
daily_volumes AS (
    SELECT
        blockchain,
        token_pair,
        stablecoin,
        swap_day,
        SUM(amount_usd) AS daily_volume
    FROM swaps
    GROUP BY 1, 2, 3, 4
),
weekly_volumes AS (
    SELECT
        blockchain,
        token_pair,
        stablecoin,
        swap_week,
        SUM(amount_usd) AS weekly_volume
    FROM swaps
    GROUP BY 1, 2, 3, 4
),
monthly_volumes AS (
    SELECT
        blockchain,
        token_pair,
        stablecoin,
        swap_month,
        SUM(amount_usd) AS monthly_volume
    FROM swaps
    GROUP BY 1, 2, 3, 4
),
swaps_summary AS (
    SELECT
        s.blockchain,
        s.token_pair,
        s.stablecoin,
        SUM(s.amount_usd) AS swap_volume,
        APPROX_PERCENTILE(s.amount_usd, 0.5) AS median_swap_amount,
        COUNT(*) AS number_of_swaps,
        APPROX_PERCENTILE(dv.daily_volume, 0.5) AS median_24h_volume,
        APPROX_PERCENTILE(wv.weekly_volume, 0.5) AS median_7d_volume,
        APPROX_PERCENTILE(mv.monthly_volume, 0.5) AS median_30d_volume
    FROM swaps s
    LEFT JOIN daily_volumes dv ON s.blockchain = dv.blockchain AND s.token_pair = dv.token_pair AND s.stablecoin = dv.stablecoin AND s.swap_day = dv.swap_day
    LEFT JOIN weekly_volumes wv ON s.blockchain = wv.blockchain AND s.token_pair = wv.token_pair AND s.stablecoin = wv.stablecoin AND s.swap_week = wv.swap_week
    LEFT JOIN monthly_volumes mv ON s.blockchain = mv.blockchain AND s.token_pair = mv.token_pair AND s.stablecoin = mv.stablecoin AND s.swap_month = mv.swap_month
    GROUP BY 1, 2, 3
)

SELECT
    blockchain,
    token_pair,
    swap_volume,
    median_swap_amount,
    number_of_swaps,
    median_24h_volume,
    median_7d_volume,
    median_30d_volume
FROM swaps_summary
ORDER BY swap_volume DESC;
