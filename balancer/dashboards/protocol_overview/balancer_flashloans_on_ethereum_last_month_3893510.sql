-- part of a query repo
-- query name: Balancer Flashloans on ethereum, last month
-- query link: https://dune.com/queries/3893510


SELECT DATE_TRUNC('day', block_time) AS day, sum(amount_usd) AS amount_usd
FROM balancer.flashloans
WHERE DATE_TRUNC('day', block_time) > now() - interval '30' day
AND blockchain = 'ethereum'
GROUP BY 1