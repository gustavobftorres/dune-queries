-- part of a query repo
-- query name: Weekly Swaps on Arbitrum
-- query link: https://dune.com/queries/3223065


SELECT date_trunc('week', block_time) AS week,  SUM(amount_usd * swap_fee) AS swap_fee, SUM(amount_usd) AS volume
FROM balancer_v2_arbitrum.trades
WHERE blockchain = 'arbitrum'
AND block_time <= TIMESTAMP '{{End date}}'
AND block_time >= TIMESTAMP '{{Start date}}' - INTERVAL '12' MONTH
GROUP BY 1