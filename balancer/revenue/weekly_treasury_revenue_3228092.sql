-- part of a query repo
-- query name: Weekly Treasury Revenue
-- query link: https://dune.com/queries/3228092


SELECT 
    date_trunc('week', day) as week, 
    sum(treasury_fee_usd) as treasury_fee
FROM balancer.protocol_fee
WHERE ('{{Blockchain}}' = 'All' OR blockchain = '{{Blockchain}}')
AND day > TIMESTAMP '{{Start Date}}'
GROUP BY 1