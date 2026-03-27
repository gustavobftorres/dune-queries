-- part of a query repo
-- query name: veBAL buyback study
-- query link: https://dune.com/queries/4706830


WITH vebal_price AS(
SELECT 
    day, 
    bpt_price AS vebal_price
FROM balancer.bpt_prices
WHERE contract_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
AND day >= NOW() - INTERVAL '6' month
AND blockchain = 'ethereum'
),

vebal_supply AS(
SELECT day, SUM(vebal_balance) AS total_vebal
FROM balancer_ethereum.vebal_balances_day
WHERE day >= NOW() - INTERVAL '6' month
GROUP BY 1
),

final AS(
SELECT
    DATE_TRUNC('month', f.day) AS day,
    p.vebal_price,
    CASE WHEN pool_id = 0xde8c195aa41c11a0c4787372defbbddaa31306d2000200000000000000000181
    THEN 'CoWSwap'
    WHEN pool_id = 0x3de27efa2f1aa663ae5d458857e731c129069f29000200000000000000000588
    THEN 'AAVE'
    WHEN pool_id = 0x596192bb6e41802428ac943d2f1476c1af25cc0e000000000000000000000659
    THEN 'Renzo'
    WHEN pool_id = 0x05ff47afada98a98982113758878f9a8b9fdda0a000000000000000000000645
    THEN 'EtherFi'    
    WHEN pool_id = 0x93d199263632a4ef4bb438f1feb99e57b4b5f0bd0000000000000000000005c2
    THEN 'wstETH-wETH'    
    END AS partner,
    SUM(f.treasury_fee_usd) AS treasury_fees,
    (SUM(f.treasury_fee_usd) / p.vebal_price) AS vebal_buyback --considering a 52 week lock
FROM balancer.protocol_fee f
JOIN vebal_price p ON p.day = DATE_TRUNC('month', f.day)
WHERE f.day >= NOW() - INTERVAL '6' month
GROUP BY 1, 2, 3)

SELECT 
    f.*,
    vebal_buyback / (total_vebal + vebal_buyback) AS vebal_power
FROM final f
JOIN vebal_supply s ON f.day = s.day
WHERE partner IS NOT NULL
ORDER BY 3 DESC