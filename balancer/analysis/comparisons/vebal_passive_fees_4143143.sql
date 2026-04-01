-- part of a query repo
-- query name: veBAL Passive Fees
-- query link: https://dune.com/queries/4143143


WITH fee_split_calculation AS (
    SELECT 
        blockchain,
        day,
        SUM(protocol_fee_collected_usd) AS total_protocol_fee,
        -- Fee split for veBAL passive fees
        CASE 
            -- BIP-19: L1 core pools + L2 pools: 75% voting incentives, 25% treasury
            WHEN day >= DATE '2022-07-03' AND day < DATE '2023-01-23'
                AND blockchain = 'ethereum' AND c.symbol IS NOT NULL THEN 0.00
            WHEN day >= DATE '2022-07-03' AND day < DATE '2023-01-23'
                AND blockchain != 'ethereum' THEN 0.00

            -- BIP-19: L1 non-core pools: 75% veBAL passive fees, 25% treasury
            WHEN day >= DATE '2022-07-03' AND blockchain = 'ethereum' AND c.symbol IS NULL THEN 0.75

            -- BIP-161: L1 core pools + L2 pools: 65% voting incentives, 35% treasury
            WHEN day >= DATE '2023-01-23' AND day < DATE '2023-07-24' 
                AND blockchain = 'ethereum' AND c.symbol IS NOT NULL THEN 0.00
            WHEN day >= DATE '2023-01-23' AND day < DATE '2023-07-24' 
                AND blockchain != 'ethereum' THEN 0.00

            -- BIP-161: L1 non-core pools: 65% veBAL passive fees, 35% treasury
            WHEN day >= DATE '2023-01-23' AND day < DATE '2023-07-24' 
                AND blockchain = 'ethereum' AND c.symbol IS NULL THEN 0.65

            -- BIP-322: L1 core pools + L2 pools: 50% voting incentives, 32.5% passive veBAL fees, 17.5% treasury
            WHEN day >= DATE '2023-07-24' AND blockchain = 'ethereum' AND c.symbol IS NOT NULL THEN 0.325
            WHEN blockchain != 'ethereum' THEN 0.325

            -- BIP-322: L1 non-core pools: 82.5% passive veBAL fees, 17.5% treasury
            WHEN day >= DATE '2023-07-24' AND blockchain = 'ethereum' AND c.symbol IS NULL THEN 0.825
        END AS veBAL_passive_fee_percentage
    FROM balancer.protocol_fee f
    LEFT JOIN dune.balancer.dataset_core_pools c
    ON c.network = f.blockchain
    AND f.pool_id = c.pool
    GROUP BY blockchain, c.symbol, day
)

SELECT 
    SUM(total_protocol_fee) AS total_protocol_fees,
    SUM(total_protocol_fee * veBAL_passive_fee_percentage) AS veBAL_passive_fees
FROM fee_split_calculation