-- part of a query repo
-- query name: AAVE statatoken pricing
-- query link: https://dune.com/queries/4308514


WITH wrap_unwrap AS(
    SELECT 
        evt_block_time,
        underlyingToken, --USDC
        wrappedToken, --StataSepUSDC,
        CAST(depositedUnderlying AS DOUBLE) / CAST(mintedShares AS DOUBLE) AS ratio
    FROM balancer_testnet_sepolia.Vault_evt_Wrap

    UNION ALL

    SELECT 
        evt_block_time,
        underlyingToken, --USDC
        wrappedToken, --StataSepUSDC,
        CAST(withdrawnUnderlying AS DOUBLE) / CAST(burnedShares AS DOUBLE) AS ratio
    FROM balancer_testnet_sepolia.Vault_evt_Unwrap   
)

SELECT * FROM wrap_unwrap