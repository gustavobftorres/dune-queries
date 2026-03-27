-- part of a query repo
-- query name: rETH - WETH swaps on 2024-05-16 00:00
-- query link: https://dune.com/queries/4221566


SELECT * FROM balancer.trades
WHERE token_pair = 'rETH-WETH'
AND block_date = TIMESTAMP '2024-05-16 00:00'
ORDER BY amount_usd DESC