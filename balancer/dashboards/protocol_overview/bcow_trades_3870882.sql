-- part of a query repo
-- query name: bcow_trades
-- query link: https://dune.com/queries/3870882


SELECT * FROM gnosis_protocol_v2_testnet_sepolia.GPv2Settlement_evt_Trade g
INNER JOIN balancer_testnet_sepolia.BCoWFactory_evt_LOG_NEW_POOL f ON g.owner = f.bpool