-- part of a query repo
-- query name: Tax on Yield vs. TVL analysis (Joining combined_incentives and dune data)
-- query link: https://dune.com/queries/4134754


WITH 
eth_prices AS(
SELECT
    DATE_TRUNC('month', minute) as month,
    APPROX_PERCENTILE(price, 0.5) as eth_price
FROM prices.usd
WHERE blockchain = 'ethereum'
AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
AND minute >= TIMESTAMP '{{start date}}'
AND minute <= TIMESTAMP '{{end date}}'
GROUP BY 1
), 

historical_bribe_premium AS (
    SELECT 
        DATE_TRUNC('month', CAST(round_end AS TIMESTAMP)) AS round_end,
        AVG(emissions_per_1) AS bribe_premium
    FROM dune.balancer.dataset_historical_bribe_premium
    GROUP BY 1
),
historical_chain_premium AS (
    SELECT
        DATE_TRUNC('month', CAST(round_end AS TIMESTAMP)) AS round_end,
        blockchain,
        AVG(premium) AS chain_premium
    FROM dune.balancer.dataset_historical_chain_premium
    WHERE blockchain = '{{blockchain}}'
    GROUP BY 1, 2
),
yield_tax AS (
    SELECT
        b.round_end AS round_end,
        CASE 
            WHEN c.blockchain IS NULL THEN 'ethereum'
            ELSE c.blockchain
        END AS blockchain,
        COALESCE(chain_premium, 1) AS chain_premium,
        bribe_premium,
        (COALESCE(chain_premium, 1) * bribe_premium * 0.25) + 0.5 AS tax_on_yield
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
        CASE WHEN '{{currency}}' = 'USD'
        THEN SUM(l.protocol_liquidity_usd) 
        ELSE SUM(l.protocol_liquidity_eth) 
        END AS tvl
    FROM yield_tax y
    JOIN balancer.liquidity l 
    ON y.round_end = l.day
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
         y.round_end,
         CASE WHEN c.chain IS NULL THEN 'ethereum' ELSE c.chain END AS blockchain,
         CASE WHEN '{{currency}}' = 'USD'
         THEN SUM(COALESCE(c.earned_fees, protocol_fees * 2)) 
         ELSE SUM(COALESCE(c.earned_fees, protocol_fees * 2)) / SUM(e.eth_price)
         END AS earned_fees,
         CASE WHEN '{{currency}}' = 'USD'
         THEN SUM(COALESCE(c.total_incentives, monthly_emissions_usd)) 
         ELSE SUM(COALESCE(c.total_incentives, monthly_emissions_usd)) / SUM(e.eth_price)
         END AS total_incentives
    FROM yield_tax y
    LEFT JOIN dune.balancer.dataset_combined_incentives c 
        ON DATE_TRUNC('month', CAST(c.end_date AS TIMESTAMP) + INTERVAL '18' day) = y.round_end
    LEFT JOIN query_4104304 q 
        ON y.round_end = q.block_month
    LEFT JOIN eth_prices e 
        ON y.round_end = e.month
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
),

final AS(
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
    f.total_incentives_growth/*,
    monthly_emissions_usd / eth_price AS monthly_emissions_eth,
    (yield_fee / eth_price + swap_fee / eth_price) / 2 AS protocol_fees_eth, */ 
FROM tvl_growth t
LEFT JOIN fees_growth f
ON t.round_end = f.round_end
AND f.blockchain = t.blockchain
ORDER BY round_end DESC)

SELECT * FROM final