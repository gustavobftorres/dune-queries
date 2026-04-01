-- part of a query repo
-- query name: Balancer 24h Volume on Mainnet
-- query link: https://dune.com/queries/31194


WITH swaps AS (
        SELECT usd_amount AS usd_amount
        FROM dex.trades
        WHERE project = 'Balancer' AND block_time > now() - interval '24h'
    )

SELECT SUM(usd_amount) AS usd_amount
FROM swaps