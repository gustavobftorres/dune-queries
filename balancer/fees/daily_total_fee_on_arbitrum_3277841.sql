-- part of a query repo
-- query name: Daily Total Fee on Arbitrum
-- query link: https://dune.com/queries/3277841


SELECT  
    day, 
    SUM(protocol_fee_collected_usd) * 2  AS total_fee --protocol fees collected + LP fees 
FROM balancer_v2_arbitrum.protocol_fee
WHERE day <= TIMESTAMP '{{End date}}' 
AND day >= TIMESTAMP '{{Start date}}'
GROUP BY 1
ORDER BY 1 ASC
