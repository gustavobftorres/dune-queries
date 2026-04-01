-- part of a query repo
-- query name: Lifetime Protocol Fees
-- query link: https://dune.com/queries/4746663


SELECT
    blockchain,
    pool_id,
    pool_symbol,
    SUM(protocol_fee_collected_usd) AS lifetime_protocol_fees,
    SUM(CASE WHEN day >= NOW() - INTERVAL '7' day THEN protocol_Fee_collected_usd END) AS protocol_fees_7d,
    SUM(CASE WHEN day >= NOW() - INTERVAL '30' day THEN protocol_Fee_collected_usd END) AS protocol_fees_30d,
    SUM(CASE WHEN day >= NOW() - INTERVAL '180' day THEN protocol_Fee_collected_usd END) AS protocol_fees_180d,
    SUM(CASE WHEN day >= NOW() - INTERVAL '365' day THEN protocol_Fee_collected_usd END) AS protocol_fees_1y
FROM balancer.protocol_fee
WHERE ('{{blockchain}}'= 'All' OR '{{blockchain}}' = blockchain)
AND protocol_Fee_collected_usd < 1e8
GROUP BY 1, 2, 3
ORDER BY 4 DESC