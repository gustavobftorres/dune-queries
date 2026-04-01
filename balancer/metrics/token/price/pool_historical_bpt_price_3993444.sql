-- part of a query repo
-- query name: Pool Historical BPT price
-- query link: https://dune.com/queries/3993444


SELECT 
    day,
    bpt_price
FROM balancer.bpt_prices
WHERE day <= TIMESTAMP '{{3. End date}}'
AND day >= TIMESTAMP '{{2. Start date}}'
AND contract_address = BYTEARRAY_SUBSTRING({{1. Pool ID}},1,20)
AND blockchain = '{{4. Blockchain}}'
GROUP BY 1, 2