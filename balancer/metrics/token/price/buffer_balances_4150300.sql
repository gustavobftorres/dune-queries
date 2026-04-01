-- part of a query repo
-- query name: Buffer Balances
-- query link: https://dune.com/queries/4150300


SELECT
    DATE_TRUNC('hour', q.evt_block_time) AS evt_block_time,
    wrappedToken,
    APPROX_PERCENTILE(wrapped_balance, 0.5) AS wrapped_balance,
    APPROX_PERCENTILE(underlying_balance, 0.5) AS underlying_balance
FROM query_4144874 q
JOIN balancer_v3_{{blockchain}}.Vault_evt_Swap s ON DATE_TRUNC('hour', q.evt_block_time) = DATE_TRUNC('hour', s.evt_block_time)
AND ({{wrapped_token}} = tokenIn OR {{wrapped_token}} = tokenOut)
JOIN balancer_v3.erc4626_token_mapping m ON q.wrappedToken = m.erc4626_token
AND q.blockchain = m.blockchain
WHERE q.wrappedToken = {{wrapped_token}}
AND q.blockchain = '{{blockchain}}'
GROUP BY 1, 2