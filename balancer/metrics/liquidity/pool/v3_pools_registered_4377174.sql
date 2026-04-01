-- part of a query repo
-- query name: V3 pools registered
-- query link: https://dune.com/queries/4377174


SELECT evt_block_time, pool, factory, 'gnosis' as blockchain FROM balancer_v3_gnosis.Vault_evt_PoolRegistered
UNION 
SELECT evt_block_time, pool, factory, 'ethereum' as blockchain FROM balancer_v3_ethereum.Vault_evt_PoolRegistered
ORDER BY evt_block_time DESC