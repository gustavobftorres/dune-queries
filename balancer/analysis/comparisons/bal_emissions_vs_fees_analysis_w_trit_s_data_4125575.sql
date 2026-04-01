-- part of a query repo
-- query name: BAL Emissions vs.  Fees Analysis (w/ Trit's data)
-- query link: https://dune.com/queries/4125575


WITH bal_emissions AS(
SELECT 
    DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS block_month,
    SUM(bal_incentives) AS bal_emissions_usd,
    SUM(total_incentives) AS total_incentives_usd
FROM dune.balancer.dataset_combined_incentives
WHERE DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) >= TIMESTAMP '{{start date}}'
AND DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) <= TIMESTAMP '{{end date}}'
GROUP BY 1
),

fees AS(
SELECT 
    DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) AS block_month,
    SUM(earned_fees) AS earned_fees
FROM dune.balancer.dataset_combined_incentives
WHERE 1 = 1
AND DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) >= TIMESTAMP '{{start date}}'
AND DATE_TRUNC('month', CAST(start_date AS TIMESTAMP)) <= TIMESTAMP '{{end date}}'
GROUP BY 1),

eth_prices AS(
SELECT
    DATE_TRUNC('month', minute) as month,
    AVG(price) as eth_price
FROM prices.usd
WHERE blockchain = 'ethereum'
AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
AND minute >= TIMESTAMP '{{start date}}'
AND minute <= TIMESTAMP '{{end date}}'
GROUP BY 1
),

tvl_2 AS(
SELECT
    day,
    SUM(protocol_liquidity_usd) AS tvl_usd,
    SUM(protocol_liquidity_eth) AS tvl_eth
FROM balancer.liquidity
WHERE day >= TIMESTAMP '{{start date}}'
AND day <= TIMESTAMP '{{end date}}'
GROUP BY 1
),

tvl AS(
SELECT
    DATE_TRUNC('month', day) AS month,
    APPROX_PERCENTILE(tvl_usd, 0.5) AS median_tvl_usd,
    APPROX_PERCENTILE(tvl_eth, 0.5) AS median_tvl_eth
FROM tvl_2
GROUP BY 1
)

SELECT
    f.block_month,
    earned_fees,
    earned_fees / eth_price AS earned_fees_eth,
    bal_emissions_usd,
    total_incentives_usd,
    bal_emissions_usd / eth_price AS bal_emissions_eth,
    total_incentives_usd / eth_price AS total_emissions_eth,
    (earned_fees) / 2 AS protocol_fees,
    (earned_fees / eth_price) / 2 AS protocol_fees_eth,
    median_tvl_usd,
    median_tvl_eth
FROM fees f
JOIN bal_emissions b ON f.block_month = b.block_month
JOIN eth_prices e ON f.block_month = e.month
JOIN tvl t ON f.block_month = t.month
WHERE 1 = 1
AND f.block_month >= TIMESTAMP '{{start date}}'
AND f.block_month <= TIMESTAMP '{{end date}}'