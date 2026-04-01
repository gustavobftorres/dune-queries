-- part of a query repo
-- query name: BAL Emissions vs.  Fees Analysis
-- query link: https://dune.com/queries/4104304


WITH bal_emissions AS(
SELECT 
    DATE_TRUNC('month', c.time) AS block_month,
    SUM(day_rate) AS monthly_emissions,
    SUM(day_rate * p.avg_price) AS monthly_emissions_usd
FROM query_2846023 c
LEFT JOIN (
    SELECT 
        DATE_TRUNC('day', minute) AS price_day,
        AVG(price) AS avg_price
    FROM prices.usd
    WHERE blockchain = 'ethereum'
    AND symbol = 'BAL'
    GROUP BY 1
) p
ON DATE_TRUNC('day', c.time) = p.price_day
WHERE c.time >= TIMESTAMP '{{start date}}'
AND c.time <= TIMESTAMP '{{end date}}'
GROUP BY 1
),

fees AS(
SELECT 
    block_month,
    SUM(total_protocol_yield_fee) AS yield_fee,
    SUM(total_protocol_swap_fee) AS swap_fee
FROM query_4104279
WHERE 1 = 1
AND total_protocol_yield_fee >= 0 
AND total_protocol_swap_fee >= 0
AND block_month >= TIMESTAMP '{{start date}}'
AND block_month <= TIMESTAMP '{{end date}}'
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
    yield_fee,
    yield_fee / eth_price AS yield_fee_eth,
    swap_fee,
    swap_fee / eth_price AS swap_fee_eth,
    monthly_emissions,
    monthly_emissions_usd,
    monthly_emissions_usd / eth_price AS monthly_emissions_eth,
    (yield_fee + swap_fee) / 2 AS protocol_fees,
    (yield_fee / eth_price + swap_fee / eth_price) / 2 AS protocol_fees_eth,
    median_tvl_usd,
    median_tvl_eth
FROM fees f
JOIN bal_emissions b ON f.block_month = b.block_month
JOIN eth_prices e ON f.block_month = e.month
JOIN tvl t ON f.block_month = t.month
WHERE 1 = 1
AND f.block_month >= TIMESTAMP '{{start date}}'
AND f.block_month <= TIMESTAMP '{{end date}}'