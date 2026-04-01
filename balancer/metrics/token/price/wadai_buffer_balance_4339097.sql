-- part of a query repo
-- query name: waDAI buffer balance
-- query link: https://dune.com/queries/4339097


SELECT 
*,
varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances
,17, 32)) / POWER(10,18) AS underlying_balance,
varbinary_to_int256(BYTEARRAY_SUBSTRING(bufferBalances
, 1, 16)) / POWER(10,18)
AS wrapped_balance
FROM balancer_testnet_sepolia.Vault_evt_LiquidityAddedToBuffer
WHERE bufferBalances IS NOT NULL
AND wrappedToken = 0xde46e43f46ff74a23a65ebb0580cbe3dfe684a17