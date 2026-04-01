-- part of a query repo
-- query name: Tax on Yield vs. TVL analysis
-- query link: https://dune.com/queries/4126956


WITH 
historical_bribe_premium AS (
    SELECT 
        round_end,
        emissions_per_1 AS bribe_premium
    FROM dune.balancer.dataset_historical_bribe_premium
),
historical_chain_premium AS (
    SELECT
        round_end,
        blockchain,
        premium AS chain_premium
    FROM dune.balancer.dataset_historical_chain_premium
),
yield_tax AS (
    SELECT
        CAST(b.round_end AS TIMESTAMP) AS round_end,
        CASE 
            WHEN c.blockchain IS NULL THEN 'ethereum'
            ELSE c.blockchain
        END AS blockchain,
        COALESCE(chain_premium, 1) AS chain_premium,
        COALESCE(bribe_premium, 1) AS bribe_premium,
        0.5 + (COALESCE(chain_premium, 1) * COALESCE(bribe_premium, 1) * (0.5 / 2)) AS tax_on_yield
    FROM historical_bribe_premium b
    LEFT JOIN historical_chain_premium c
    ON b.round_end = c.round_end
),
tvl_data AS (
    SELECT
        y.round_end,
        l.blockchain,
        y.tax_on_yield,
        y.tax_on_yield - 1 AS tax_on_yield_pct,
        SUM(l.protocol_liquidity_eth) AS tvl
    FROM yield_tax y
    JOIN balancer.liquidity l 
    ON y.round_end /*+ INTERVAL '14' DAY*/ = l.day
    AND l.blockchain = y.blockchain
    WHERE y.round_end >= TIMESTAMP '{{start date}}'
      AND y.round_end <= TIMESTAMP '{{end date}}'
      AND y.blockchain = '{{blockchain}}'
    GROUP BY 1, 2, 3
),

tvl_growth AS (
    SELECT
        round_end,
        blockchain,
        tvl,
        tax_on_yield,
        tax_on_yield_pct,
        LAG(tvl) OVER (PARTITION BY blockchain ORDER BY round_end) AS previous_tvl,
        CASE 
            WHEN LAG(tvl) OVER (PARTITION BY blockchain ORDER BY round_end) IS NOT NULL THEN
                (tvl - LAG(tvl) OVER (PARTITION BY blockchain ORDER BY round_end)) / LAG(tvl) OVER (PARTITION BY blockchain ORDER BY round_end)
            ELSE NULL
        END AS tvl_growth
    FROM tvl_data
),

fees AS (
    SELECT
         CAST(c.end_date AS TIMESTAMP) + INTERVAL '18' day AS round_end,
         chain AS blockchain,
         SUM(earned_fees) AS earned_fees,
         SUM(total_incentives) AS total_incentives
    FROM dune.balancer.dataset_combined_incentives c
    GROUP BY 1, 2
),

fees_growth AS (
    SELECT
        round_end,
        blockchain,
        earned_fees,
        LAG(earned_fees) OVER (PARTITION BY blockchain ORDER BY round_end) AS previous_earned_fees,
        CASE 
            WHEN LAG(earned_fees) OVER (PARTITION BY blockchain ORDER BY round_end) IS NOT NULL THEN
                (earned_fees - LAG(earned_fees) OVER (PARTITION BY blockchain ORDER BY round_end)) / LAG(earned_fees) OVER (PARTITION BY blockchain ORDER BY round_end)
            ELSE NULL
        END AS earned_fees_growth,
        total_incentives,
        LAG(total_incentives) OVER (PARTITION BY blockchain ORDER BY round_end) AS previous_total_incentives,
        CASE 
            WHEN LAG(total_incentives) OVER (PARTITION BY blockchain ORDER BY round_end) IS NOT NULL THEN
                (total_incentives - LAG(total_incentives) OVER (PARTITION BY blockchain ORDER BY round_end)) / LAG(total_incentives) OVER (PARTITION BY blockchain ORDER BY round_end)
            ELSE NULL
        END AS total_incentives_growth
    FROM fees
)

SELECT 
    t.round_end,
    t.blockchain,
    t.tvl,
    t.tax_on_yield,
    t.tax_on_yield_pct,
    t.previous_tvl,
    t.tvl_growth, 
    f.earned_fees,
    f.total_incentives,
    f.previous_earned_fees,
    f.earned_fees_growth,
    f.previous_total_incentives,
    f.total_incentives_growth
FROM tvl_growth t
LEFT JOIN fees_growth f
ON t.round_end = f.round_end
AND f.blockchain = t.blockchain
ORDER BY round_end DESC;