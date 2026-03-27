-- part of a query repo
-- query name: Balancer Flashloans, last 30 days
-- query link: https://dune.com/queries/3896093


SELECT DATE_TRUNC('day', block_time) AS day, blockchain, sum(amount_usd) AS amount_usd
FROM balancer.flashloans
WHERE DATE_TRUNC('day', block_time) > now() - interval '30' day
GROUP BY 1, 2
