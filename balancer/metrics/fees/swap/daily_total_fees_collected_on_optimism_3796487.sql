-- part of a query repo
-- query name: Daily Total Fees Collected on Optimism
-- query link: https://dune.com/queries/3796487


SELECT day, SUM(protocol_fee_collected_usd) AS total_fee
FROM balancer_v2_optimism.protocol_fee
WHERE day <= TIMESTAMP '{{End date}}' 
AND day >= TIMESTAMP '{{Start date}}'
GROUP BY 1
ORDER BY 1 ASC
