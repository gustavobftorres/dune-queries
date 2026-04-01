-- part of a query repo
-- query name: Balancer Pool Token Balances
-- query link: https://dune.com/queries/4739347


SELECT DISTINCT
    DATE_TRUNC('hour', evt_block_time) AS hour,
    token_symbol,
    SUM(delta_amount) OVER (PARTITION BY token_symbol ORDER BY DATE_TRUNC('hour', evt_block_time)) AS balance
FROM balancer.token_balance_changes t
WHERE (blockchain = '{{blockchain}}')
AND pool_address = {{balancer pool}}
ORDER BY 1 DESC