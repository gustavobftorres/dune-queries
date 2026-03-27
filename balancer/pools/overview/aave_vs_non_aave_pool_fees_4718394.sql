-- part of a query repo
-- query name: AAVE vs. Non-AAVE pool fees
-- query link: https://dune.com/queries/4718394


SELECT 
    block_month,
    SUM(CASE WHEN pool_address =  0x3de27efa2f1aa663ae5d458857e731c129069f29 THEN total_protocol_swap_fee ELSE 0 END) AS aave_swap_fee,
    SUM(CASE WHEN pool_address =  0x3de27efa2f1aa663ae5d458857e731c129069f29 THEN total_protocol_yield_fee ELSE 0 END) AS aave_yield_fee,
    SUM(CASE WHEN pool_address !=  0x3de27efa2f1aa663ae5d458857e731c129069f29 THEN total_protocol_swap_fee ELSE 0 END) AS non_aave_swap_fee,
    SUM(CASE WHEN pool_address !=  0x3de27efa2f1aa663ae5d458857e731c129069f29 THEN total_protocol_yield_fee ELSE 0 END) AS non_aave_yield_fee,
    SUM(total_protocol_swap_fee) AS total_protocol_swap_fee,
    SUM(total_protocol_yield_fee) AS total_protocol_yield_fee
FROM query_4104279
WHERE total_protocol_swap_fee > 0 AND total_protocol_yield_fee > 0
AND block_month >= TIMESTAMP '2024-01-01 00:00'
AND block_month <= TIMESTAMP '2024-12-01 00:00'
GROUP BY 1
ORDER BY 1 DESC