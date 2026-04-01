-- part of a query repo
-- query name: Treasury revenue study, by partner on the last 6 months
-- query link: https://dune.com/queries/4671558


WITH bal_prices AS(
    SELECT
        APPROX_PERCENTILE(price,0.5) AS median_bal_price
    FROM prices.usd
    WHERE 1 = 1
    AND minute >= NOW() - INTERVAL '6' month
    AND blockchain = 'ethereum'
    AND symbol = 'BAL'
),

final AS(
SELECT
    p.median_bal_price,
    CASE WHEN pool_id = 0xde8c195aa41c11a0c4787372defbbddaa31306d2000200000000000000000181
    THEN 'CoWSwap'
    WHEN pool_id = 0x3de27efa2f1aa663ae5d458857e731c129069f29000200000000000000000588
    THEN 'AAVE'
    WHEN pool_id = 0x9232a548dd9e81bac65500b5e0d918f8ba93675c000200000000000000000423
    THEN 'LIT'
    WHEN pool_id = 0x32df62dc3aed2cd6224193052ce665dc181658410002000000000000000003bd
    THEN 'Radiant'
    WHEN pool_id = 0x36be1e97ea98ab43b4debf92742517266f5731a3000200000000000000000466
    THEN 'Alchemix'
    WHEN pool_id = 0x596192bb6e41802428ac943d2f1476c1af25cc0e000000000000000000000659
    THEN 'Renzo'
    WHEN pool_id = 0x05ff47afada98a98982113758878f9a8b9fdda0a000000000000000000000645
    THEN 'EtherFi'    
    END AS partner,
    SUM(f.treasury_fee_usd) AS treasury_fees,
    (SUM(f.treasury_fee_usd) / p.median_bal_price) * 0.001 AS point_one_pct_buyback,
    (SUM(f.treasury_fee_usd) / p.median_bal_price) * 0.005 AS point_five_pct_buyback,
    (SUM(f.treasury_fee_usd) / p.median_bal_price) * 0.01 AS one_pct_buyback
FROM balancer.protocol_fee f
CROSS JOIN bal_prices p 
WHERE f.day >= NOW() - INTERVAL '6' month
GROUP BY 1, 2)

SELECT * FROM final
WHERE partner IS NOT NULL
ORDER BY 3 DESC