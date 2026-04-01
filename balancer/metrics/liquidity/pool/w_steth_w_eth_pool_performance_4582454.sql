-- part of a query repo
-- query name: (w)stETH - (W)ETH Pool Performance
-- query link: https://dune.com/queries/4582454


WITH liquidity AS(
SELECT 
    pool_address,
    SUM(protocol_liquidity_usd) AS tvl,
    CASE WHEN pool_address = 0x93d199263632a4ef4bb438f1feb99e57b4b5f0bd
    THEN 0.0001
    WHEN pool_address = 0xc4ce391d82d164c166df9c8336ddf84206b2f812
    THEN 0.00002
    END AS swap_fee
FROM balancer.liquidity
WHERE day = CURRENT_DATE
AND pool_address IN (0x93d199263632a4ef4bb438f1feb99e57b4b5f0bd, 0xc4ce391d82d164c166df9c8336ddf84206b2f812)
GROUP BY 1

UNION

SELECT
    pool_address,
    tvl,
    swap_fee
FROM (VALUES 
     (0xdc24316b9ae028f1497c275eb9192a3ea0f67022, 170717117.76, 0.0001),
     (0x109830a1aaad605bbf02a9dfa7b0b92ec2fb7daa, 13600000, 0.0001),
     (0x0b1a513ee24972daef112bc777a5610d4325c9e7, 68795190, 0.0001)
    ) AS temp_table (pool_address, tvl, swap_fee)
)


SELECT 
    project,
    project_contract_address,
    CASE WHEN token_pair IS NULL THEN 'ETH-stETH'
    ELSE token_pair
    END AS token_pair,
    swap_fee,
    tvl AS tvl_usd,
    SUM(amount_usd) AS volume_7d,
    SUM(amount_usd) / tvl AS liquidity_utilization,
    COUNT(DISTINCT tx_to) AS unique_addresses,
    APPROX_PERCENTILE(amount_usd, 0.5) AS median_swap
FROM dex.trades
LEFT JOIN liquidity ON project_contract_address = pool_address
WHERE 1 = 1 
AND blockchain = 'ethereum'
AND block_date >= CURRENT_DATE - INTERVAL '7' DAY
AND (token_pair = 'WETH-wstETH' OR
project_contract_address IN (0xdc24316b9ae028f1497c275eb9192a3ea0f67022, 0xc4ce391d82d164c166df9c8336ddf84206b2f812))
GROUP BY 1, 2, 3, 4, 5
ORDER BY 6 DESc
LIMIT 5