-- part of a query repo
-- query name: veBAL buyback study - impact on 80/20 pool
-- query link: https://dune.com/queries/4708527


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
SELECT day, SUM(vebal_balance) AS total_vebal
FROM balancer_ethereum.vebal_balances_day
WHERE day >= NOW() - INTERVAL '12' month
GROUP BY 1
),

consolidated AS(
SELECT
    DATE_TRUNC('month', f.day) AS day,
    p.vebal_price,
    CASE WHEN pool_id = 0xde8c195aa41c11a0c4787372defbbddaa31306d2000200000000000000000181
    THEN 'CoWSwap'
    WHEN pool_id = 0x3de27efa2f1aa663ae5d458857e731c129069f29000200000000000000000588
    THEN 'AAVE'
    WHEN pool_id = 0x05ff47afada98a98982113758878f9a8b9fdda0a000000000000000000000645
    THEN 'EtherFi'    
    WHEN pool_id = 0x93d199263632a4ef4bb438f1feb99e57b4b5f0bd0000000000000000000005c2
    THEN 'Lido'    
    END AS partner,
    SUM(f.treasury_fee_usd) AS redirected_fees,
    (SUM(f.treasury_fee_usd) / p.vebal_price) AS vebal_buyback --considering a 52 week lock
FROM balancer.protocol_fee f
JOIN vebal_price p ON p.day = DATE_TRUNC('month', f.day)
WHERE f.day >= NOW() - INTERVAL '12' month
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
    l.day,
    partner,
    redirected_fees,
    vebal_buyback,
    vebal_price,
    vebal_power,
    redirected_fees * 0.8 AS bal_purchased_usd,
    redirected_fees * 0.8 / median_bal_price AS bal_purchased,
    median_bal_price,
    ((redirected_fees * 0.8 / median_bal_price) / (CASE WHEN token_symbol = 'BAL' THEN token_balance END)) AS bal_bought_impact_in_pool,
    CASE WHEN token_symbol = 'BAL' THEN token_balance END AS bal_balance_in_pool,
    redirected_fees * 0.2 AS weth_purchased_usd,
    redirected_fees * 0.2 / median_weth_price AS weth_purchased,
    median_weth_price
FROM final f
LEFT JOIN bal_prices b ON f.day = b.month
LEFT JOIN weth_prices w ON f.day = w.month
LEFT JOIN balancer.liquidity l
ON l.day = f.day
AND l.pool_address = 0x5c6Ee304399DBdB9C8Ef030aB642B10820DB8F56)

SELECT * FROM decorated 
WHERE bal_balance_in_pool IS NOT NULL
AND day < TIMESTAMP '2025-02-01 00:00'
ORDER BY 1 DESC, 3 DESC
