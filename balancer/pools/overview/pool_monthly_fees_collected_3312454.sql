-- part of a query repo
-- query name: Pool Monthly Fees Collected
-- query link: https://dune.com/queries/3312454


SELECT 
    DATE_TRUNC('month', day) as month, 
    pool_symbol, 
    SUM(protocol_fee_collected_usd) as protocol_fee_collected
FROM balancer.protocol_fee
WHERE day <= TIMESTAMP '{{3. End date}}'
AND day >= TIMESTAMP '{{2. Start date}}'
AND pool_id = {{1. Pool ID}}
AND blockchain = '{{4. Blockchain}}'
GROUP BY 1, 2