-- part of a query repo
-- query name: v3_vault_exits
-- query link: https://dune.com/queries/4109263


SELECT * 
FROM balancer_testnet_sepolia.Vault_call_removeLiquidity
ORDER BY call_block_time DESC