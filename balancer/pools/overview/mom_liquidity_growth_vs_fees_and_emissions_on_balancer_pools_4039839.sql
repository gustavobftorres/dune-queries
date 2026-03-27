-- part of a query repo
-- query name: MoM Liquidity Growth vs. Fees and Emissions on Balancer Pools
-- query link: https://dune.com/queries/4039839


WITH monthly_median_liquidity AS (
    SELECT
        date_trunc('month', day) AS month,
        pool_address,
        pool_symbol,
        blockchain,
        APPROX_PERCENTILE(pool_liquidity_eth, 0.5) AS median_pool_liquidity_eth
    FROM balancer.liquidity
    GROUP BY 1, 2, 3, 4
),

mom_median_growth AS (
    SELECT
        month,
        pool_address,
        pool_symbol,
        blockchain,
        median_pool_liquidity_eth,
        LAG(median_pool_liquidity_eth) OVER (PARTITION BY pool_address, blockchain ORDER BY month) AS previous_median_liquidity,
        (median_pool_liquidity_eth - LAG(median_pool_liquidity_eth) OVER (PARTITION BY pool_address, blockchain ORDER BY month)) / LAG(median_pool_liquidity_eth) OVER (PARTITION BY pool_address, blockchain ORDER BY month) AS mom_growth
    FROM monthly_median_liquidity
),

mom_emissions_growth AS (
    SELECT
        g.month,
        g.pool_address,
        g.pool_symbol,
        g.blockchain,
        g.median_pool_liquidity_eth,
        g.mom_growth,
        q.monthly_emissions,
        q.monthly_emissions_usd,
        q.monthly_fees,
        LAG(q.monthly_emissions) OVER (PARTITION BY g.pool_address, g.blockchain ORDER BY g.month) AS previous_monthly_emissions,
        LAG(q.monthly_emissions_usd) OVER (PARTITION BY g.pool_address, g.blockchain ORDER BY g.month) AS previous_monthly_emissions_usd,
        LAG(q.monthly_fees) OVER (PARTITION BY g.pool_address, g.blockchain ORDER BY g.month) AS previous_monthly_fees,
        (q.monthly_emissions - LAG(q.monthly_emissions) OVER (PARTITION BY g.pool_address, g.blockchain ORDER BY g.month)) / LAG(q.monthly_emissions) OVER (PARTITION BY g.pool_address, g.blockchain ORDER BY g.month) AS mom_emissions_growth,
        (q.monthly_emissions_usd - LAG(q.monthly_emissions_usd) OVER (PARTITION BY g.pool_address, g.blockchain ORDER BY g.month)) / LAG(q.monthly_emissions_usd) OVER (PARTITION BY g.pool_address, g.blockchain ORDER BY g.month) AS mom_emissions_usd_growth,
        (q.monthly_fees - LAG(q.monthly_fees) OVER (PARTITION BY g.pool_address, g.blockchain ORDER BY g.month)) / LAG(q.monthly_fees) OVER (PARTITION BY g.pool_address, g.blockchain ORDER BY g.month) AS mom_fees_growth
    FROM mom_median_growth g
    LEFT JOIN query_3480969 q ON g.pool_address = q.pool_address
    AND g.blockchain = q.blockchain
    AND g.month = q.day
)

SELECT
    month,
    pool_address,
    pool_symbol,
    blockchain,
    median_pool_liquidity_eth,
    mom_growth AS mom_liquidity_growth,
    monthly_emissions,
    COALESCE(mom_emissions_growth, 0) AS mom_emissions_growth,
    monthly_emissions_usd,
    COALESCE(mom_emissions_usd_growth, 0) AS mom_emissions_usd_growth,
    monthly_fees,
    COALESCE(mom_fees_growth, 0) AS mom_fees_growth
FROM mom_emissions_growth
WHERE mom_growth IS NOT NULL
AND month > DATE_TRUNC('month', NOW()) - interval '12' month
AND month < DATE_TRUNC('month', now()) 
AND is_finite(mom_emissions_usd_growth)
ORDER BY 1 DESC, 5 DESC, 6 DESC