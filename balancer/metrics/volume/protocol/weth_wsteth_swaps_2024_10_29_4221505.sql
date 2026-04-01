-- part of a query repo
-- query name: WETH-wstETH swaps 2024-10-29
-- query link: https://dune.com/queries/4221505


SELECT
    *
FROM balancer.trades
WHERE token_pair = 'WETH-wstETH'
AND block_date = TIMESTAMP '2024-10-29 00:00'
ORDER BY amount_usd DESC