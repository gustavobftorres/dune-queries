-- part of a query repo
-- query name: Protocol Fees Overview by Pool Type
-- query link: https://dune.com/queries/3548050


SELECT 
    pool_type,
    SUM(protocol_fee_collected_usd) AS all_time_protocol,
    SUM(CASE WHEN day >= current_date - interval '30' day THEN protocol_fee_collected_usd ELSE 0 END) AS "30_day_protocol",
    SUM(CASE WHEN day >= current_date - interval '7' day THEN protocol_fee_collected_usd ELSE 0 END) AS "7_day_protocol",
    SUM(treasury_fee_usd) AS all_time_treasury,
    SUM(CASE WHEN day >= current_date - interval '30' day THEN treasury_fee_usd ELSE 0 END) AS "30_day_treasury",
    SUM(CASE WHEN day >= current_date - interval '7' day THEN treasury_fee_usd ELSE 0 END) AS "7_day_treasury"
FROM balancer.protocol_fee f
WHERE pool_type IS NOT NULL
AND ('{{3. Blockchain}}' = 'All' OR f.blockchain = '{{3. Blockchain}}')
AND protocol_fee_collected_usd < 1000000000
GROUP BY pool_type
HAVING SUM(protocol_fee_collected_usd) > 0
ORDER BY 2 DESC;