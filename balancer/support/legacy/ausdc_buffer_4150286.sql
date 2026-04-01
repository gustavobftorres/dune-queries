-- part of a query repo
-- query name: aUSDC Buffer
-- query link: https://dune.com/queries/4150286


SELECT
    day,
    wrappedToken,
    wrapped_balance / POWER(10,6) AS wrapped_balance,
    SUM (CASE WHEN tokenIn = 0x8a88124522dbbf1e56352ba3de1d9f78c143751e THEN amountIN / POWER(10,6)
    WHEN tokenOut = 0x8a88124522dbbf1e56352ba3de1d9f78c143751e THEN amountOut / POWER(10,6)
    END) AS swap_volume
FROM query_4144874 q
LEFT JOIN balancer_testnet_sepolia.Vault_evt_Swap s ON q.day = s.evt_block_date
AND (q.wrappedToken = tokenIn OR q.wrappedToken = tokenOut)
WHERE q.wrappedToken = 0x8a88124522dbbf1e56352ba3de1d9f78c143751e
GROUP BY 1, 2, 3
