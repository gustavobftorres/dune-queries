-- part of a query repo
-- query name: Yield vs. Swap Fees, monthly
-- query link: https://dune.com/queries/4136649


SELECT 
    block_month,
    SUM(total_protocol_yield_fee) AS yield_fee,
    SUM(total_protocol_swap_fee) AS swap_fee
FROM query_4104279
WHERE 1 = 1
AND total_protocol_yield_fee >= 0 
AND total_protocol_swap_fee >= 0
AND block_month >= TIMESTAMP '{{start date}}'
AND block_month <= TIMESTAMP '{{end date}}'
AND ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
GROUP BY 1