-- part of a query repo
-- query name: v3_vault_swaps
-- query link: https://dune.com/queries/4109264


SELECT * 
FROM balancer_testnet_sepolia.Vault_evt_Swap
ORDER BY evt_block_time DESC