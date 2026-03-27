-- part of a query repo
-- query name: Treasury revenue study
-- query link: https://dune.com/queries/4671431


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
)

SELECT
    DATE_TRUNC('month', f.day) AS month,
    p.median_bal_price,
    SUM(f.treasury_fee_usd) AS treasury_fees,
    (SUM(f.treasury_fee_usd) / p.median_bal_price) * 0.001 AS point_one_pct_buyback,
    (SUM(f.treasury_fee_usd) / p.median_bal_price) * 0.005 AS point_five_pct_buyback,
    (SUM(f.treasury_fee_usd) / p.median_bal_price) * 0.01 AS one_pct_buyback
FROM balancer.protocol_fee f
JOIN bal_prices p ON DATE_TRUNC('month', f.day) = p.month
WHERE f.day >= NOW() - INTERVAL '12' month
GROUP BY 1, 2
ORDER BY 1 DESC