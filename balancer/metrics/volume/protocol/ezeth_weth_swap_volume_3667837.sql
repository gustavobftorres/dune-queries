-- part of a query repo
-- query name: ezETH-WETH swap volume
-- query link: https://dune.com/queries/3667837


SELECT
    DATE_TRUNC('hour', block_time) AS block_time,
    sum(amount_usd) AS volume
FROM balancer.trades
WHERE token_pair = 'ezETH-WETH'
AND block_date = TIMESTAMP '2024-04-24'
AND blockchain = 'ethereum'
GROUP BY 1