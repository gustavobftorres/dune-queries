-- part of a query repo
-- query name: Balancer CoWSwap AMM Pools Created, by Blockchain
-- query link: https://dune.com/queries/3954624


WITH
arb_pools as(
SELECT 
date_trunc('week',evt_block_time) as week, count(contract_address) as pools_registered, 'arbitrum' as blockchain
FROM b_cow_amm_arbitrum.BCoWFactory_evt_LOG_NEW_POOL
GROUP BY 1),

eth_pools as(
SELECT 
date_trunc('week',evt_block_time) as week, count(contract_address) as pools_registered, 'ethereum' as blockchain
FROM b_cow_amm_ethereum.BCoWFactory_evt_LOG_NEW_POOL
GROUP BY 1
),

gno_pools as(
SELECT 
date_trunc('week',evt_block_time) as week, count(contract_address) as pools_registered, 'gnosis' as blockchain
FROM b_cow_amm_gnosis.BCoWFactory_evt_LOG_NEW_POOL
GROUP BY 1),

base_pools as(
SELECT 
date_trunc('week',evt_block_time) as week, count(contract_address) as pools_registered, 'base' as blockchain
FROM b_cow_amm_base.BCoWFactory_evt_LOG_NEW_POOL
GROUP BY 1),

pools_registered as(
SELECT * FROM arb_pools
UNION ALL
SELECT * FROM eth_pools
UNION ALL
SELECT * FROM gno_pools
UNION ALL
SELECT * FROM base_pools
)

SELECT *,
     blockchain || 
        CASE 
            WHEN blockchain = 'arbitrum' THEN ' 🟦'
            WHEN blockchain = 'ethereum' THEN ' Ξ'
            WHEN blockchain = 'gnosis' THEN ' 🟩'
            WHEN blockchain = 'base' THEN ' 🟨'
        END 
    AS blockchain 
FROM pools_registered
WHERE week >= TIMESTAMP '{{1. Start date}}' 

