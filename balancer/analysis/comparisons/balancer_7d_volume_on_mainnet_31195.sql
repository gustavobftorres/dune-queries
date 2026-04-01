-- part of a query repo
-- query name: Balancer 7d Volume on Mainnet
-- query link: https://dune.com/queries/31195


SELECT SUM(usd_amount) AS usd_amount
FROM dex.trades
WHERE project = 'Balancer' AND block_time > now() - interval '7d'