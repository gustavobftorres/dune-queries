-- part of a query repo
-- query name: Balancer Pools Created, by Blockchain
-- query link: https://dune.com/queries/2617646


WITH
arb_pools as(
SELECT 
date_trunc('week',evt_block_time) as week, count(contract_address) as pools_registered, 'arbitrum' as blockchain
FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered
GROUP BY 1),

eth_pools as(
SELECT 
date_trunc('week',evt_block_time) as week, count(contract_address) as pools_registered, 'ethereum' as blockchain
FROM balancer_v2_ethereum.Vault_evt_PoolRegistered
GROUP BY 1
),

gno_pools as(
SELECT 
date_trunc('week',evt_block_time) as week, count(contract_address) as pools_registered, 'gnosis' as blockchain
FROM balancer_v2_gnosis.Vault_evt_PoolRegistered
GROUP BY 1),

opt_pools as(
SELECT 
date_trunc('week',evt_block_time) as week, count(contract_address) as pools_registered, 'optimism' as blockchain
FROM balancer_v2_optimism.Vault_evt_PoolRegistered
GROUP BY 1),

pol_pools as(
SELECT 
date_trunc('week',evt_block_time) as week, count(contract_address) as pools_registered, 'polygon' as blockchain
FROM balancer_v2_polygon.Vault_evt_PoolRegistered
GROUP BY 1),

ava_pools as(
SELECT 
date_trunc('week',evt_block_time) as week, count(contract_address) as pools_registered, 'avalanche_c' as blockchain
FROM balancer_v2_avalanche_c.Vault_evt_PoolRegistered
GROUP BY 1),

base_pools as(
SELECT 
date_trunc('week',evt_block_time) as week, count(contract_address) as pools_registered, 'base' as blockchain
FROM balancer_v2_base.Vault_evt_PoolRegistered
GROUP BY 1),

zkevm_pools as(
SELECT 
date_trunc('week',evt_block_time) as week, count(contract_address) as pools_registered, 'zkevm' as blockchain
FROM balancer_v2_zkevm.Vault_evt_PoolRegistered
GROUP BY 1),

pools_registered as(
SELECT * FROM arb_pools
UNION ALL
SELECT * FROM eth_pools
UNION ALL
SELECT * FROM opt_pools
UNION ALL
SELECT * FROM pol_pools
UNION ALL
SELECT * FROM gno_pools
UNION ALL
SELECT * FROM ava_pools
UNION ALL
SELECT * FROM base_pools
UNION ALL
SELECT * FROM zkevm_pools)

SELECT *,
     blockchain || 
        CASE 
            WHEN blockchain = 'arbitrum' THEN ' 🟦'
            WHEN blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN blockchain = 'base' THEN ' 🟨'
            WHEN blockchain = 'ethereum' THEN ' Ξ'
            WHEN blockchain = 'gnosis' THEN ' 🟩'
            WHEN blockchain = 'optimism' THEN ' 🔴'
            WHEN blockchain = 'polygon' THEN ' 🟪'
            WHEN blockchain = 'zkevm' THEN ' 🟣'
        END 
    AS blockchain 
FROM pools_registered
WHERE week >= TIMESTAMP '{{2. Start date}}' 

