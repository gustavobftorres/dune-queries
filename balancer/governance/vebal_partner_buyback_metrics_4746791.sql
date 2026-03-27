-- part of a query repo
-- query name: veBAL partner buyback metrics
-- query link: https://dune.com/queries/4746791


WITH bal_prices AS(
    SELECT
        DATE_TRUNC('month', minute) AS month,
        APPROX_PERCENTILE(price,0.5) AS median_bal_price
    FROM prices.usd
    WHERE 1 = 1
    AND minute >= NOW() - INTERVAL '12' month
    AND blockchain = 'ethereum'
    AND symbol = 'BAL'
    GROUP BY 1
),

weth_prices AS(
    SELECT
        DATE_TRUNC('month', minute) AS month,
        APPROX_PERCENTILE(price,0.5) AS median_weth_price
    FROM prices.usd
    WHERE 1 = 1
    AND minute >= NOW() - INTERVAL '12' month
    AND blockchain = 'ethereum'
    AND symbol = 'WETH'
    GROUP BY 1
),

vebal_price AS(
SELECT 
    day, 
    bpt_price AS vebal_price
FROM balancer.bpt_prices
WHERE contract_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
AND day >= NOW() - INTERVAL '12' month
AND blockchain = 'ethereum'
),

vebal_supply AS(
SELECT 
    day, 
    SUM(vebal_balance) AS total_vebal
FROM balancer_ethereum.vebal_balances_day
WHERE day >= NOW() - INTERVAL '12' month
GROUP BY 1
),

vebal_fees AS(
SELECT
    date_trunc('month', f.day) AS day,
    SUM(CASE WHEN c.symbol IS NOT NULL THEN protocol_fee_collected_usd * .125
     WHEN c.symbol IS NULL THEN protocol_fee_collected_usd * .825
     END) AS fees_to_vebal
FROM balancer.protocol_fee f
LEFT JOIN dune.balancer.dataset_core_pools c 
ON f.blockchain = c.network
AND f.pool_id = c.pool
WHERE f.day >= NOW() - INTERVAL '12' month
AND protocol_fee_collected_usd < 1e8
GROUP BY 1
),

bal_emissions AS(
SELECT
    DATE_TRUNC('month', time) AS month,
    SUM(day_rate) AS month_rate
FROM query_3140829
WHERE time >= NOW() - INTERVAL '12' month
GROUP BY 1
),

consolidated AS(
SELECT
    DATE_TRUNC('month', f.day) AS day,
    p.vebal_price,   
    '{{partner}}' AS partner,
    SUM(f.protocol_fee_collected_usd) AS protocol_fees,
    SUM(f.treasury_fee_usd) AS redirected_fees,
    (SUM(f.treasury_fee_usd) / p.vebal_price) AS vebal_buyback --considering a 52 week lock
FROM balancer.protocol_fee f
JOIN vebal_price p ON p.day = DATE_TRUNC('month', f.day)
WHERE f.day >= NOW() - INTERVAL '12' month
AND f.pool_id = {{partner_pool}}
GROUP BY 1, 2, 3),

final AS(
SELECT 
    f.*,
    vebal_buyback / (total_vebal + vebal_buyback) AS vebal_power
FROM consolidated f
JOIN vebal_supply s ON f.day = s.day
WHERE partner IS NOT NULL
ORDER BY 3 DESC),

decorated AS(
SELECT DISTINCT
    f.day,
    partner,
    protocol_fees,
    redirected_fees,
    vebal_buyback,
    vebal_price,
    vebal_power,
    redirected_fees * 0.8 AS bal_purchased_usd,
    redirected_fees * 0.8 / median_bal_price AS bal_purchased,
    median_bal_price,
    redirected_fees * 0.2 AS weth_purchased_usd,
    redirected_fees * 0.2 / median_weth_price AS weth_purchased,
    median_weth_price,
    e.month_rate,
    SUM(vebal_power) OVER (ORDER BY f.day),
    e.month_rate * SUM(vebal_power) OVER (ORDER BY f.day) AS bal_emissions_commanded,
    e.month_rate * SUM(vebal_power) OVER (ORDER BY f.day) * median_bal_price AS bal_emissions_commanded_usd
FROM final f
LEFT JOIN bal_prices b ON f.day = b.month
LEFT JOIN weth_prices w ON f.day = w.month
LEFT JOIN bal_emissions e ON f.day = e.month)

SELECT 
    partner,
    d.day,
    DATE_FORMAT(d.day, '%Y-%m') AS month,
    SUM(protocol_fees) AS protocol_fees_usd,
    SUM(redirected_fees) AS redirected_fees_usd,
    SUM(redirected_fees) AS fees_delta_to_be_offset, --considering 1:1 offset of redirected incentive fees to partners
    SUM(vebal_buyback) AS vebal_buyback,
    SUM(vebal_power) AS vebal_power,
    SUM(fees_to_vebal *  vebal_power) AS vebal_fees_received_usd,
    SUM(bal_emissions_commanded_usd) AS bal_emissions_commanded_usd,
    SUM(bal_emissions_commanded) AS bal_emissions_commanded,
    SUM(redirected_fees * 0.8) AS bal_purchased_usd,
    SUM(redirected_fees * 0.8 / median_bal_price) AS bal_purchased,
    SUM(redirected_fees * 0.2) AS weth_purchased_usd,
    SUM(redirected_fees * 0.2 / median_weth_price) AS weth_purchased
FROM decorated d
INNER JOIN vebal_fees f
ON f.day = d.day
GROUP BY 1, 2, 3
ORDER BY 2 DESC