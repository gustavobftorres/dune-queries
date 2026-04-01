-- part of a query repo
-- query name: ARB Incentives vs. Fees Collected, per week
-- query link: https://dune.com/queries/3684456


WITH arb_price AS(
SELECT 
    DATE_TRUNC('week', minute) AS week,
    APPROX_PERCENTILE(price, 0.5) AS price
FROM prices.usd
WHERE symbol = 'ARB' AND blockchain = 'arbitrum'
GROUP BY 1
),

arb_incentives AS (
SELECT
    date_trunc('week', evt_block_time) AS week,
    SUM(amount / pow(10, 18)) AS token_amount
FROM balancer_v2_arbitrum.ChildChainGaugeInjector_evt_EmissionsInjection i
WHERE token = 0x912ce59144191c1204e64559fe8253a0e49e6548
GROUP BY 1),

arb_incentives_usd AS(
SELECT
    q.week,
    SUM(token_amount) AS weekly_incentives_arb,
    SUM(token_amount * price) AS weekly_incentives_arb_usd
FROM arb_incentives q
LEFT JOIN arb_price a ON q.week = a.week
GROUP BY 1
),

arb_fees AS(
SELECT 
    DATE_TRUNC('week', day) AS week,
    sum(protocol_fee_collected_usd) AS fees
FROM balancer_v2_arbitrum.protocol_fee
GROUP BY 1
)

SELECT 
    CAST(t.week AS TIMESTAMP) AS week,
    a.weekly_incentives_arb,
    t.fees
FROM arb_fees t
LEFT JOIN arb_incentives_usd a
ON t.week = a.week
WHERE t.week >= NOW() - INTERVAL '1' year