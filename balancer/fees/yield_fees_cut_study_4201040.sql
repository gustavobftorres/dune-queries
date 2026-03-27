-- part of a query repo
-- query name: Yield Fees Cut Study
-- query link: https://dune.com/queries/4201040


WITH fees AS(
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
GROUP BY 1)

SELECT
    block_month,
    yield_fee + swap_fee AS total_fee,
    yield_fee * {{cut rate in yield fees}} + swap_fee AS proposed_total_fee,
    swap_fee,
    yield_fee,
    yield_fee * {{cut rate in yield fees}} AS proposed_yield_fee,
    yield_fee / swap_fee AS swap_to_yield_fee_ratio,
    yield_fee * {{cut rate in yield fees}} / swap_fee AS proposed_swap_to_yield_fee_ratio
FROM fees
ORDER BY 1 DESC