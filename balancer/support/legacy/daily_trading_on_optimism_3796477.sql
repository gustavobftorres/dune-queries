-- part of a query repo
-- query name: Daily Trading on Optimism
-- query link: https://dune.com/queries/3796477


SELECT date_trunc('day', block_time) AS day, SUM(amount_usd * swap_fee) AS swap_fee, SUM(amount_usd) AS volume
FROM balancer_v2_optimism.trades
WHERE block_time <= TIMESTAMP '{{End date}}'
AND block_time >= TIMESTAMP '{{Start date}}'
GROUP BY 1
